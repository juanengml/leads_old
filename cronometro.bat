@echo off

:hora
cls
echo.
echo As horas atuais são: %time:~0,8%
timeout /t 1 > Nul
goto :hora