import kaggle

kaggle.api.authenticate()

kaggle.api.dataset_download_files('olistbr/brazilian-ecommerce', path='.', unzip=True)
