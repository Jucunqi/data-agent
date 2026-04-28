from huggingface_hub import AsyncInferenceClient, InferenceClient

from app.clients.my_inference_client import MyInferenceClient
from app.conf.app_config import EmbeddingConfig, app_config


class EmbeddingClientManager():

    def __init__(self, config: EmbeddingConfig):
        self.client: MyInferenceClient | None = None
        self.config = config

    def _get_url(self):
        return f"http://{self.config.host}:{self.config.port}"

    def init(self):
        self.client = MyInferenceClient(base_url=self._get_url())


embedding_client_manager = EmbeddingClientManager(app_config.embedding)

if __name__ == '__main__':
    embedding_client_manager.init()
    client = embedding_client_manager.client
    text = ["What is deep learning?","hello,world"]
    query_result = client.embed_documents(text)
    print(query_result[0][:3])
    print(query_result[1][:3])
