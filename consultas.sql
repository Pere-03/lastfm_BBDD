USE Lastfm;

-- CONSULTA 1
-- Obtener el número total de usuarios distintos que han
-- escuchado a Led Zeppelin.

SELECT COUNT(DISTINCT e.USERID) AS Usuarios_unicos
FROM escuchas AS e
INNER JOIN canciones AS c ON c.ID = e.TRAID
INNER JOIN artistas AS a ON c.ARTID = a.ID
WHERE a.ARTNAME = 'Led Zeppelin';


-- CONSULTA 2
-- Obtener el número total de mujeres que han escuchado a Madonna

SELECT COUNT(DISTINCT e.USERID) AS Usuarios_unicos
FROM escuchas AS e
INNER JOIN canciones AS c ON c.ID = e.TRAID
INNER JOIN artistas AS a ON c.ARTID = a.ID
INNER JOIN usuarios AS u ON e.USERID = u.ID
WHERE a.ARTNAME = 'Madonna'
AND u.GENERO = 'f';


-- Consulta 3
-- Obtener el número total de usuarios que o bien son de España (Spain)
-- o bien han escuchado a ’Red Hot Chili Peppers’

-- Suponemos que siguen siendo usuarios únicos


SELECT COUNT(*) AS NUM_USUARIOS
FROM (
-- Usuarios de españa
SELECT DISTINCT u.ID
FROM usuarios AS u
WHERE u.PAIS = 'Spain'

UNION

-- Usuarios que han escuchado a ’Red Hot Chili Peppers’
SELECT DISTINCT e.USERID
FROM escuchas AS e
INNER JOIN canciones AS c ON c.ID = e.TRAID
INNER JOIN artistas AS a ON c.ARTID = a.ID
WHERE a.ARTNAME = 'Red Hot Chili Peppers') AS tmp;


-- CONSULTA 4
-- Obtener la media del total de escuchas de los usuarios

WITH escuchas_usuario AS
(
SELECT e.USERID, COUNT(e.ID) AS ESCUCHAS
FROM escuchas AS e
GROUP BY e.USERID
)
SELECT ROUND(AVG(ESCUCHAS)) AS MEDIA_ESCUCHAS
FROM escuchas_usuario;


-- CONSULTA 5
-- Obtener los usuarios cuyo número total de escuchas superan
-- a la media del total de escuchas de los usuarios.

WITH escuchas_usuario AS
(
SELECT e.USERID, COUNT(e.ID) AS ESCUCHAS
FROM escuchas AS e
GROUP BY e.USERID
)
SELECT eu.USERID, eu.ESCUCHAS
FROM escuchas_usuario AS eu
WHERE eu.ESCUCHAS > (SELECT AVG(ESCUCHAS)
					FROM escuchas_usuario);


-- CONSULTA 6
-- Número total de escuchas por país

SELECT IFNULL(u.PAIS, 'UNKNOWN'), COUNT(e.ID) AS escuchas_pais
FROM escuchas AS e
INNER JOIN usuarios AS u ON u.ID = e.USERID
GROUP BY u.PAIS;


-- CONSULTA 7
-- Obtener las 15 canciones que tienen un mayor número de escuchas de
-- usuarios distintos (muestra el nombre de la canción y el número de
-- usuarios distintos que la han escuchado).


SELECT c.TRANAME, COUNT(DISTINCT e.USERID) AS escuchas_unicas
FROM escuchas AS e
INNER JOIN canciones AS c ON e.TRAID = c.ID
GROUP BY c.TRANAME
ORDER BY escuchas_unicas DESC
LIMIT 15;


-- CONSULTA 8
-- Obtener el porcentaje de escuchas del total que han realizado los usuarios cuya
-- edad supera la media de edad de todos los usuarios. Para esta
-- consulta, ignora a aquellos usuarios cuya edad es 0 o nula.

-- Aunque sabemos que hay 1000000 escuchas, las calcularemos igualente

-- Agrupado por usuarios
SELECT e.USERID, (COUNT(e.ID)/(SELECT COUNT(e2.ID) FROM escuchas AS e2))*100 AS porcentaje_escuchas
FROM escuchas AS e
INNER JOIN usuarios AS u ON u.ID = e.USERID
WHERE u.EDAD > (SELECT AVG(u2.EDAD)
				FROM usuarios AS u2
				WHERE u2.EDAD <> 0) -- Los valores nulos NO se usan de por si
GROUP BY e.USERID;

-- Total

SELECT SUM(tmp.porcentaje_escuchas)
FROM (SELECT (COUNT(e.ID)/(SELECT COUNT(e2.ID) FROM escuchas AS e2))*100 AS porcentaje_escuchas
		FROM escuchas AS e
		INNER JOIN usuarios AS u ON u.ID = e.USERID
		WHERE u.EDAD > (SELECT AVG(u2.EDAD)
						FROM usuarios AS u2
						WHERE u2.EDAD <> 0) -- Los valores nulos NO se usan de por si
		GROUP BY e.USERID) AS tmp;
 



