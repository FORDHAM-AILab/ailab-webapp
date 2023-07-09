import hashlib

import urllib.request

occu = {}
for i in range(500):
    contents = urllib.request.urlopen("https://0ijq1i6sp1.execute-api.us-east-1.amazonaws.com/dev/stream").read()
    d=contents.decode("utf-8")
    if d not in occu: occu[d]=1
    else: occu[d] += 1

print({k: v for k, v in sorted(occu.items(), key=lambda item: item[1])})





