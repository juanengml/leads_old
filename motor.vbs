Set WshShell = WScript.CreateObject("WScript.Shell")
WshShell.Run "python -m flask run --host=0.0.0.0", 0