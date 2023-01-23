import django_keycloak_auth.clients
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.shortcuts import reverse
from django.db import models
from django import forms
import uuid
import keycloak.exceptions


def sync_resource_to_keycloak(self, display_name, scopes, urn, view_name, super_save, args, kwargs):
    uma_client = django_keycloak_auth.clients.get_uma_client()
    token = django_keycloak_auth.clients.get_access_token()
    created = False

    if not self.pk:
        created = True
    super_save(*args, **kwargs)

    create_kwargs = {
        "name": self.id,
        "displayName": f"{display_name}: {str(self)}",
        "ownerManagedAccess": True,
        "scopes": scopes,
        "type": urn,
        "uri": reverse(view_name, args=(self.id,)) if view_name else None,
    }

    if created or not self.resource_id:
        if self.user:
            create_kwargs['owner'] = self.user.username

        d = uma_client.resource_set_create(
            token,
            **create_kwargs
        )
        self.resource_id = d['_id']
        super_save()
    else:
        try:
            uma_client.resource_set_update(
                token,
                id=self.resource_id,
                **create_kwargs
            )
        except keycloak.exceptions.KeycloakClientError:
            pass


def delete_resource(resource_id):
    uma_client = django_keycloak_auth.clients.get_uma_client()
    token = django_keycloak_auth.clients.get_access_token()
    uma_client.resource_set_delete(token, resource_id)


def get_object_ids(access_token, resource_type, action):
    scope_name = f"{action}-{resource_type}"
    permissions = django_keycloak_auth.clients.get_authz_client().get_permissions(access_token)
    permissions = permissions.get("permissions", [])
    permissions = filter(lambda p: scope_name in p.get('scopes', []), permissions)
    object_ids = list(map(lambda p: p['rsid'], permissions))
    return object_ids


def eval_permission(token, resource, scope, submit_request=False):
    resource = str(resource)
    permissions = django_keycloak_auth.clients.get_authz_client().get_permissions(
        token=token,
        resource_scopes_tuples=[(resource, scope)],
        submit_request=submit_request
    )

    for permission in permissions.get('permissions', []):
        for scope in permission.get('scopes', []):
            if permission.get('rsid') == resource and scope == scope:
                return True

    return False


def get_resource_owner(resource_id):
    uma_client = django_keycloak_auth.clients.get_uma_client()
    token = django_keycloak_auth.clients.get_access_token()
    resource = uma_client.resource_set_read(token, resource_id)
    owner = resource.get("owner", {}).get("id")
    user = get_user_model().objects.filter(username=owner).first()
    return user


class TypedUUIDField(models.Field):
    def __init__(self, data_type, **kwargs):
        self.data_type = data_type
        kwargs["default"] = self.default_value
        super().__init__(**kwargs)

    def default_value(self):
        val = uuid.uuid4()
        return f"{self.data_type}_{val.hex}"

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        kwargs["data_type"] = self.data_type
        del kwargs["default"]
        return name, path, args, kwargs

    def db_type(self, connection):
        return 'uuid'

    def get_internal_type(self):
        return 'CharField'

    def from_db_value(self, value, expression, connection):
        if value is None:
            return value
        uuid_value = self._to_uuid(value)
        return f"{self.data_type}_{uuid_value.hex}"

    def _to_uuid(self, value):
        if isinstance(value, uuid.UUID):
            uuid_value = value
        else:
            prefix = f"{self.data_type}_"
            if value.startswith(prefix):
                act_value = value[len(prefix):]
            else:
                act_value = value

            try:
                uuid_value = uuid.UUID(act_value)
            except (AttributeError, ValueError):
                raise ValidationError(
                    '“%(value)s” is not a valid ID.',
                    code='invalid',
                    params={'value': value},
                )

        return uuid_value

    def to_python(self, value):
        if value is None:
            return value

        return f"{self.data_type}_{self._to_uuid(value).hex}"

    def get_prep_value(self, value):
        value = super().get_prep_value(value)
        return self._to_uuid(value)

    def get_db_prep_value(self, value, connection, prepared=False):
        if value is None:
            return None

        value = self._to_uuid(value)

        if connection.features.has_native_uuid_field:
            return value
        return value.hex

    def formfield(self, **kwargs):
        return super().formfield(**{
            'form_class': forms.CharField,
            **kwargs,
        })
