# Estructura del proyecto

En esta repo solo esta el codigo propiamente con el pom.xml. Para que funcione correctamente tienen que seguir esta estructura de carpetas:

Creamos en el disco raiz "development" (C:\development)          

Creamos estas carpetas:

C:\development\codes\supra                         (Aqui descomprimimos el zip "example" que mando Jon)

C:\development\maven-repository\example

C:\development\settings\example>\settings.xml       (Aqui va el archivo .xml que mando Jon)

Ahora entramos a la carpeta descomprimida example y borramos el archivo que se la carpeta "scala". 

Ahora en esta dirección (C:\development\codes\supra\example), creamos un nuevo repo de git y conectamos con este repo remoto y descargamos los archivos:

`git remote add origin https://gitlab.bluetab.net/team-supra/supra-shop
` 
`git branch -M main
`
`git pull origin main`



