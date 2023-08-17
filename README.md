# Actualizaciones

**17-08-23 : La escritura en stage no se hace correctamente, genera archivos en blanco. Revisar metodo .isEmpty**

# Estructura del proyecto

En esta repo solo esta el codigo propiamente con el pom.xml. Para que funcione correctamente tienen que seguir esta estructura de carpetas:

Creamos en el disco raiz "development" (C:\development)          

Creamos estas carpetas:

C:\development\codes                               (Aqui descomprimimos el zip "example" que mando Jon)

C:\development\maven-repository\example

C:\development\settings\example\settings.xml       (Aqui va el archivo .xml que mando Jon)

Ahora entramos a la carpeta descomprimida _example_ y borramos la carpeta _scala_. 

En esta dirección (C:\development\codes\supra\example), creamos un nuevo repo de git, nos conectamos con este repo remoto y descargamos los archivos:

`git remote add origin https://gitlab.bluetab.net/team-supra/supra-shop
` 
`git branch -M main
`
`git pull origin main`

Importante: Descargar el archivo _ojdbc6-11.2.0.3.jar_ en C:\development\codes 


