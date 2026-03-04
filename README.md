# api-demo
Sample code for using the Serafis API


## Auth setup

```bash
export SERAFIS_KEY="your_key_id"
export SERAFIS_SECRET="your_key_secret"
```

## Transcript downloader

```bash
python3 samples/transcripts.py
```

## Known issues

### SSL certificate error on macOS

If you see `urllib.error.URLError: <urlopen error [SSL: CERTIFICATE_VERIFY_FAILED]>` when running a script, your Python installation is missing root CA certificates. This is common with Python installed from the [python.org](https://www.python.org) macOS installer, which bundles its own OpenSSL without pre-installed certificates.

To fix, run the certificate install script that ships with your Python version:

```bash
/Applications/Python\ 3.13/Install\ Certificates.command
```

Adjust the path if you're using a different Python version (e.g. `Python 3.12`). This only needs to be run once.