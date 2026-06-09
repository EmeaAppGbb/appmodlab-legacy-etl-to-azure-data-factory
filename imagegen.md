# Image Generation Prompts - SSIS to Azure Data Factory

## Thumbnail Prompt

**Use the following prompt across all image generators to create a thumbnail for this lab:**

### Prompt (for all generators):

> A professional tech illustration showing data pipeline modernization - on the left, old SSIS packages represented as rigid metal pipes with data flowing through them; on the right, modern Azure Data Factory represented as elegant flowing light streams in the cloud. Include visual elements of data transformation (gears turning into flowing particles). Blue and orange color palette with Azure branding elements. 16:9 aspect ratio, clean modern design suitable as a repository thumbnail.

### Settings:
- **Aspect Ratio:** 16:9 (landscape)
- **Resolution:** 1792x1024 or similar
- **Style:** Professional tech illustration, clean, modern

### Generators to use:
1. **Google Gemini Pro** (Imagen 3)
2. **Azure OpenAI GPT-Image-2** (via Azure AI Foundry)
3. **Microsoft Image Creator** (Bing/Designer)

### Output:
Save generated images to:
- `assets/thumbnail-gemini.png`
- `assets/thumbnail-gpt-image.png`
- `assets/thumbnail-msdesigner.png`

After selecting the best one, rename to `assets/thumbnail.png` and update the `thumbnail` field in `appmodlab.md`.
