from abc import ABC, abstractmethod
import json
import logging
import requests

from fastapi import HTTPException
from msal import ConfidentialClientApplication
from oauthlib.oauth2 import WebApplicationClient
from starlette.responses import RedirectResponse

from fermi_backend.webapp import cache
from fermi_backend.webapp import config
from fermi_backend.webapp.exceptions import (
    DiscoveryDocumentError,
    ProviderConnectionError,
    UnauthorizedUser,
    UnknownAuthenticationProvider,
)
from fermi_backend.webapp.webapp_models.auth_models import (
    ExternalAuthToken,
    ExternalUser,
)
from fermi_backend.webapp.auth.util import create_state_csrf_token

logger = logging.getLogger(__name__)


async def get_auth_provider(auth_provider: str):
    """
    Works out the correct authentication provider that needs
    to be contacted, based on the provider name that was
    passed as an argument.
    Raises:
        backend.exceptions.UnknownAuthenticationProvider
    """
    for provider_cls in AuthProvider.__subclasses__():
        try:
            if await provider_cls.meets_condition(auth_provider):
                return provider_cls(client_id=provider_cls.client_id)
        except KeyError:
            continue

    raise UnknownAuthenticationProvider(auth_provider)


class AuthProvider(ABC):
    """ Authentication providers interface """

    def __init__(self, client_id: str):
        # OAuth 2 client setup
        self.auth_client = WebApplicationClient(client_id)

    @staticmethod
    @abstractmethod
    async def meets_condition(self):
        """
        Checks whether this type of authentication provider
        matches any of the ones defined in the configuration.
        Makes sure the correct provider will be instantiated.
        """
        ...

    @abstractmethod
    async def get_user(self, auth_token: ExternalAuthToken) -> ExternalUser:
        """
        Receives an authentication token from an external provider (i.e Google, Microsoft)
        and exchanges it for an access token. Then, it retrieves the user's details from
        the external providers user-info endpoint.
        Args:
            auth_token: The authentication token received from the external provider
        Returns:
            external_user: A user object with the details of the user's account as
                            it is stored in the external provider's system.
        """
        ...

    @abstractmethod
    async def get_request_uri(self) -> str:
        """
        Returns the external provider's URL for sign in.
        For example, for Google this will be a URL that will
        bring up the Google sign in pop-up window and prompt
        the user to log-in.
        Returns:
            request_uri: Sign in pop-up URL
        """
        ...

    @abstractmethod
    async def _get_discovery_document(self) -> dict:
        """
        Returns the OpenId configuration information from the Auth provider.
        This is handy in order to get the:
            1. token endpoint
            2. authorization endpoint
            3. user info endpoint
        Returns:
            discovery_document: The configuration dictionary
        """
        ...


class GoogleAuthProvider(AuthProvider):
    """
    Google authentication class for authenticating users and
    requesting user's information via an OpenIdConnect flow.
    """
    client_id = config.GOOGLE_CLIENT_ID

    @staticmethod
    async def meets_condition(auth_provider):
        return auth_provider == config.GOOGLE

    async def get_user(self, auth_token: ExternalAuthToken) -> ExternalUser:
        # Get Google's endpoints from discovery document
        discovery_document = await self._get_discovery_document()
        try:
            token_endpoint = discovery_document["token_endpoint"]
            userinfo_endpoint = discovery_document["userinfo_endpoint"]
        except KeyError as exc:
            raise DiscoveryDocumentError(f"Could not parse Google's discovery document: {repr(exc)}")

        # Request access_token from Google
        token_url, headers, body = self.auth_client.prepare_token_request(
            token_endpoint,
            redirect_url=config.GOOGLE_REDIRECT_URL,
            code=auth_token.code
        )

        try:
            token_response = requests.post(
                token_url,
                headers=headers,
                data=body,
                auth=(config.GOOGLE_CLIENT_ID, config.GOOGLE_CLIENT_SECRET),
            )

            self.auth_client.parse_request_body_response(json.dumps(token_response.json()))

        except Exception as exc:
            raise ProviderConnectionError(f"Could not get Google's access token: {repr(exc)}")

        # Request user's information from Google
        uri, headers, body = self.auth_client.add_token(userinfo_endpoint)
        userinfo_response = requests.get(uri, headers=headers, data=body)

        if userinfo_response.json().get("email_verified"):
            email = userinfo_response.json()["email"]
            sub_id = userinfo_response.json()["sub"]
            username = userinfo_response.json()["given_name"]
        else:
            raise UnauthorizedUser("User account not verified by Google.")

        external_user = ExternalUser(
            email=email,
            username=username,
            external_sub_id=sub_id,
        )

        return external_user

    async def get_request_uri(self):
        discovery_document = await self._get_discovery_document()

        try:
            authorization_endpoint = discovery_document["authorization_endpoint"]
        except KeyError as exc:
            raise ProviderConnectionError(f"Could not parse Google's discovery document: {repr(exc)}")

        state_csrf_token = await create_state_csrf_token()

        request_uri = self.auth_client.prepare_request_uri(
            authorization_endpoint,
            state=state_csrf_token,
            redirect_uri=config.GOOGLE_REDIRECT_URL,
            scope=["openid", "email", "profile"],
        )

        return request_uri, state_csrf_token

    async def _get_discovery_document(self) -> dict:
        try:
            discovery_document = requests.get(config.GOOGLE_DISCOVERY_URL).json()
        except Exception as exc:
            raise ProviderConnectionError(f"Could not get Google's discovery document: {repr(exc)}")

        return discovery_document

