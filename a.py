  python3 -c "
  import sys, platform
  print('Python:', sys.version)
  print('OS:', platform.system(), platform.machine())

  try:
      import selenium; print('Selenium:', selenium.__version__)
  except: print('Selenium: NOT INSTALLED')

  import os
  chrome = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
  print('Chrome exists:', os.path.isfile(chrome))

  import subprocess
  try:
      r = subprocess.run([chrome, '--version'], capture_output=True, text=True, timeout=5)
      print('Chrome version:', r.stdout.strip())
  except Exception as e:
      print('Chrome error:', e)
  "
