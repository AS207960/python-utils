from rest_framework import authentication
from rest_framework import exceptions
from django.contrib.auth import get_user_model
import django_keycloak_auth.clients
import django.conf
import keycloak
import jose.jwt
import dataclasses
import datetime
import requests

_pat_certs = None


def get_pat_certs():
    global _pat_certs
    if not _pat_certs:
        r = requests.get(f"{django.conf.settings.PAT_URL}/pat_jwks.json", timeout=5)
        r.raise_for_status()
        _pat_certs = r.json()
    return _pat_certs


class BearerAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        token = request.META.get('HTTP_AUTHORIZATION')
        if not token:
            return None
        if not token.startswith("Bearer "):
            return None

        token = token[len("Bearer "):]

        try:
            claims = django_keycloak_auth.clients.verify_token(token)
        except keycloak.exceptions.KeycloakClientError:
            raise exceptions.AuthenticationFailed('Invalid token')

        user = get_user_model().objects.filter(username=claims["sub"]).first()
        if not user:
            oidc_profile = django_keycloak_auth.clients.update_or_create_user_and_oidc_profile(id_token_object=claims)
            user = oidc_profile.user

        try:
            django_keycloak_auth.clients.get_active_access_token(user.oidc_profile)
        except django_keycloak_auth.clients.TokensExpired:
            user.oidc_profile.access_token = token
            user.oidc_profile.expires_before = datetime.datetime.fromtimestamp(claims["exp"])\
                .replace(tzinfo=datetime.timezone.utc)
            user.oidc_profile.save()

        return user, OAuthToken(token=token, claims=claims)


class PATAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        pat_token = request.META.get('HTTP_AUTHORIZATION')
        if not pat_token:
            return None
        if not pat_token.startswith("X-AS207960-PAT "):
            return None

        pat_token = pat_token[len("X-AS207960-PAT "):]

        pat_certs = get_pat_certs()
        try:
            jose.jwt.decode(pat_token, pat_certs)
        except jose.jwt.JWTError:
            raise exceptions.AuthenticationFailed('Invalid token')

        client_token = django_keycloak_auth.clients.get_access_token()
        r = requests.post(f"{django.conf.settings.PAT_URL}/verify_pat/", headers={
            "Authorization": f"Bearer {client_token}",
        }, timeout=5, data={
            "token": pat_token
        })
        r.raise_for_status()
        pat_data = r.json()

        if not pat_data.get("active"):
            raise exceptions.AuthenticationFailed('Invalid token')

        user = get_user_model().objects.filter(username=pat_data["sub"]).first()
        if not user:
            raise exceptions.AuthenticationFailed('Invalid token')

        try:
            token = django_keycloak_auth.clients.get_active_access_token(user.oidc_profile)
        except django_keycloak_auth.clients.TokensExpired:
            raise exceptions.AuthenticationFailed('Invalid token')

        certs = django_keycloak_auth.clients.get_openid_connect_client().certs()
        try:
            claims = jose.jwt.decode(token, certs, options={
                "verify_aud": False
            })
        except jose.jwt.JWTError:
            raise exceptions.AuthenticationFailed('Invalid token')

        return user, OAuthToken(token=token, claims=claims)


class SessionAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        user = getattr(request._request, 'user', None)
        if not user or not user.is_active:
            return None
        self.enforce_csrf(request)
        token = django_keycloak_auth.clients.get_active_access_token(user.oidc_profile)

        certs = django_keycloak_auth.clients.get_openid_connect_client().certs()
        try:
            claims = jose.jwt.decode(token, certs, options={
                "verify_aud": False
            })
        except jose.jwt.JWTError:
            raise exceptions.AuthenticationFailed('Invalid token')

        return user, OAuthToken(token=token, claims=claims)

    def enforce_csrf(self, request):
        check = authentication.CSRFCheck()
        check.process_request(request)
        reason = check.process_view(request, None, (), {})
        if reason:
            raise exceptions.PermissionDenied('CSRF Failed: %s' % reason)


@dataclasses.dataclass
class OAuthToken:
    token: str
    claims: dict