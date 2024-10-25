# RobustCrawl
A library for robust cralwer based on proxy pool and token bucket, support browser and requests

# Install
``` 
pip install robust_crawl
playwright install chrome
export OPENAI_API_KEY="yourkey"  
export OPENAI_API_BASE="your base" # optional
```

brew install go

brew install mihomo

set imported proxy file (.yml / .yaml) in ./config

# Config
save it in ./config/robust_crawl_config.json

```json
{
    "wait_sec": 10,
    "max_concurrent_requests": 500,
    "GPT": {
        "model_type": "gpt-3.5-turbo"
    },
    "TokenBucket": {
        "tokens_per_minute": 20,
        "bucket_capacity": 5,
        "url_specific_tokens": {
            "export.arxiv": {
                "tokens_per_minute": 19,
                "bucket_capacity": 1
            }
        }
    },
    "Proxy": {
        "is_enabled": true,
        "start_port": 33333,
        "proxies_dir": "./proxies"
    },
    "ContextPool": {
        "num_contexts": 2,
        "work_contexts": 15,
        "have_proxy": true,
        "duplicate_proxies": false,
        "ensure_none_proxies": true,
        "download_pdf": false,
        "downloads_path": "./output/browser_downloads",
        "preference_path": "./output/broswer_config",
        "context_lifetime": 60,
        "context_cooling_time": 1
    }
}
```