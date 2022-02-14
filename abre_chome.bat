echo off
title Abrir e Fechar Firefox Infinitamente
goto main

:main
cls
python siluma_usuario_abre_navegador.py

timeout /t 60
goto main