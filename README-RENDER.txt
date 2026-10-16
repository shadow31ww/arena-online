MULTIJUGADOR ONLINE - DESPLIEGUE EN RENDER

Incluye index.html (tu juego), server.js y package.json.

1) Entra a https://github.com y crea un repositorio nuevo. Puedes llamarlo arena-online.
2) Sube los TRES archivos de esta carpeta a la raíz del repositorio: index.html, server.js y package.json.
3) En https://render.com entra con GitHub y pulsa New + > Web Service.
4) Selecciona el repositorio que acabas de crear.
5) Configura:
   - Name: arena-online (o el nombre que quieras)
   - Runtime: Node
   - Build Command: npm install
   - Start Command: npm start
   - Instance Type: el plan disponible que prefieras (el gratuito puede dormir/iniciar lento según las condiciones actuales de Render).
6) Pulsa Create Web Service y espera a que Render indique que está desplegado.
7) Abre la URL HTTPS que Render te da. Esa es la página del juego y también el servidor de salas.
8) Tú y tu amigo abren ESA MISMA URL desde sus casas. En Multijugador, anfitrión crea una sala; comparte el código; el otro lo escribe y se une.

NOTAS:
- Este servidor está diseñado para el multijugador de dos jugadores que usa el cliente actual.
- En esta primera versión el código de sala se usa como nombre; la contraseña es opcional.
- Render debe mantener el servicio accesible. Si el servicio gratuito entra en reposo, al primer jugador puede tocarle esperar a que despierte.
- Para una partida real, ambos deben usar la URL pública de Render, no abrir el HTML como archivo local ni la URL LAN.
