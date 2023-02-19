# BBDD-lastfm
Creacion de una base de datos de canciones a partir de un dataset.

Este deberá descargarse a partir del siguiente enlace: http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html


Esta se creará en un servidor mysql, por lo que habrá que introducir tanto unusuario como su clave en el archivo config.txt.
Para un correcto funcionamiento, el usuario deberá tener permisos de create, insert, drop, select.
Además, será necesario introducir la ruta en la están los .tsv's. Por defecto, esta será ./data
Si se encuentran en el directorio actual, limitese a indicar '.'