@ECHO ON
@echo off
echo.
echo.
echo.
:inicio
cls
ECHO ----- Nome do Computador -----
Hostname
echo.
ECHO ----- Usuario Conectado -----
ECHO %username%
echo.
ECHO ----- Data/Hora -----
ECHO %date% %time%
echo.
ECHO ----- Dominio -----

ECHO %userdomain%
echo.
echo.
timeout /t 30
goto :inicio