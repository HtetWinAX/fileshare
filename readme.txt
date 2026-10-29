Fast LAN file transfer. No dependencies beyond Python 3.

On the Mac (files live here):
  python3 main.py --root /path/to/data

On the Linux box (receives them):
  curl -fsSO http://192.168.1.3:8000/pull.py
  python3 pull.py http://192.168.1.3:8000 /destination

Browser UI:  http://192.168.1.3:8000/
Full guide:  README.md

(Do not use uvicorn any more; main.py is a standalone stdlib server.)
