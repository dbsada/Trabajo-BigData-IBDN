```sh
brew install --cask google-cloud-sdk
```

```sh
gcloud auth login
```

```sh
gcloud config set project `nombre-del-proyecto`
```

`gcloud config get-value project` deberia devolver el nombre del proyecto si todo ha ido bien.


```sh
cp .env.example .env
```

modifica 

```env
GCP_PROJECT=            # <--- Set your GCP project ID here
```