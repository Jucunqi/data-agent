from huggingface_hub import InferenceClient, AsyncInferenceClient


class MyInferenceClient:

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.inference_client: InferenceClient = InferenceClient(base_url=self.base_url)
        self.async_inference_client: AsyncInferenceClient = AsyncInferenceClient(base_url=self.base_url)

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Async Call to HuggingFaceHub's embedding endpoint for embedding search docs.

        Args:
            texts: The list of texts to embed.

        Returns:
            List of embeddings, one for each text.

        """
        # replace newlines, which can negatively affect performance.
        texts = [text.replace("\n", " ") for text in texts]
        responses = self.inference_client.feature_extraction(text=texts)
        return responses.tolist()

    async def aembed_documents(self, texts: list[str]) -> list[list[float]]:
        """Async Call to HuggingFaceHub's embedding endpoint for embedding search docs.

        Args:
            texts: The list of texts to embed.

        Returns:
            List of embeddings, one for each text.

        """
        # replace newlines, which can negatively affect performance.
        texts = [text.replace("\n", " ") for text in texts]
        responses = await self.async_inference_client.feature_extraction(text=texts)
        return responses.tolist()

    def embed_query(self, text: str) -> list[float]:
        """Call out to HuggingFaceHub's embedding endpoint for embedding query text.

        Args:
            text: The text to embed.

        Returns:
            Embeddings for the text.

        """
        return self.embed_documents([text])[0]

    async def aembed_query(self, text: str) -> list[float]:
        """Async Call to HuggingFaceHub's embedding endpoint for embedding query text.

        Args:
            text: The text to embed.

        Returns:
            Embeddings for the text.

        """
        return (await self.aembed_documents([text]))[0]
