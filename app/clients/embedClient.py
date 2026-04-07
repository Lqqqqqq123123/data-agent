from langchain_huggingface.embeddings import HuggingFaceEndpointEmbeddings

from app.conf.app_config import app_config, EmbeddingConfig


class EmbeddingClientManager:
    def __init__(self):
        self.client: HuggingFaceEndpointEmbeddings | None = None
        self.config: EmbeddingConfig | None = None


    def _get_url(self):
        return f'http://{self.config.host}:{self.config.port}'

    def init(self):
        self.config = app_config.embedding
        self.client = HuggingFaceEndpointEmbeddings(
            model = self._get_url(),
            # model=self.config.model,
        )

# 向外暴露全局单例示例
embedding_client_manager = EmbeddingClientManager()

if __name__ == '__main__':
    embedding_client_manager.init()
    client = embedding_client_manager.client

    resp = client.aembed_query("test")

    print(resp)

