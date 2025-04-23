import os
import openai
from httpx_auth import OAuth2ClientCredentials
from langchain_openai import ChatOpenAI


class ApolloOpenAI(openai.OpenAI):
    """
    ApolloOpenAI is a subclass of openai.OpenAI that initializes the client with self-refreshing OAuth2 credentials.
    """

    def __init__(self, client_id: str, client_secret: str, token_url: str, base_url: str, **kwargs):
        """
        Initialize the ApolloOpenAI client with OAuth2 credentials.

        Args:
            client_id (str): The client ID for OAuth2 authentication.
            client_secret (str): The client secret for OAuth2 authentication.
            token_url (str): The URL to request the OAuth2 token.
            base_url (str): The base URL for the Apollo API.
            **kwargs: Additional keyword arguments to pass to the parent class initializer.

        """
        self._apollo_credentials = OAuth2ClientCredentials(
            client_id=client_id,
            client_secret=client_secret,
            token_url=token_url,
        )
        _ = kwargs.pop("api_key", None)
        super().__init__(api_key="API_KEY", base_url=base_url, **kwargs)

    @property
    def custom_auth(self) -> OAuth2ClientCredentials:
        return self._apollo_credentials

client = ApolloOpenAI(
    client_id="8ebdb955-72fc-46b2-8b60-41e4fedda31b",
    client_secret="6d83d133-8594-41f2-bbb9-7d4cf76c9850",
    #token_url="https://api-mgmt.boehringer-ingelheim.com:8065/api/oauth/token",
    token_url="https://api-gw.boehringer-ingelheim.com/api/oauth/token",
    base_url="https://api-gw.boehringer-ingelheim.com/llm-api",
)

# Get the list of available model names
#models = client.models.list()
#print([model.id for model in models])

# # Make an LLM request
response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        #{"role": "system", "content": "You are a chatbot."},
        {"role": "user", "content": "What are stem cells?"},
    ],
    max_tokens=100
    ,  # optional parameter - it represents the maximum length of the response the model will generate, where each token is roughly 3-4 characters of English text. This parameter acts as a ceiling - the model will generate fewer tokens than specified, but never more.
    temperature=0.5,
)
print(response)
#
# # Make an embeddings request
# response = client.embeddings.create(
#     model="openai-text-embedding-3-small",
#     inputs=["Hello, how are you?"],
# )
# print(response)
