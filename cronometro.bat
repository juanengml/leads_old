@echo off

:hora
cls
echo.
echo As horas atuais s�o: %time:~0,8%
timeout /t 1 > Nul
goto :hora