from datetime import datetime as dt


def filtrar_escuchas(path: str):
    '''
    Recogera el primer millon de escuchas con canciones no nulas.
    Guardará los artistas, las canciones y la info de las escuchas

    Args:
        path: ubicación del fichero
        "userid-timestamp-artid-artname-traid-traname.tsv"

    Returns:
        escuchas: lista con cada escucha
        canciones: lista con las canciones
        artistas: lista con los artistas
    '''
    escuchas = ['id\tuser_id\tid_cancion\ttimestamp\n']
    canciones = ['id\tid_cancion\tnombre_cancion\tid_artista\n']
    artistas = ['id\tid_artista\tnombre_artista\n']
    artistas_guardados = {}
    canciones_guardadas = {}

    with open(
                f'{path}/userid-timestamp-artid-artname-traid-traname.tsv',
                'r', encoding='utf-8') as fichero:
        i = 1
        id_artista = 1
        id_cancion = 1
        while i <= 1000000:
            linea = fichero.readline()
            fragmentos = linea.split('\t')
            fragmentos[3] += '\n'

            if fragmentos[4]:
                # Veamos si ya existe el artista
                artista = artistas_guardados.get(fragmentos[2], False)
                if artista:
                    artista_actual = str(artista)
                else:
                    artistas_guardados[fragmentos[2]] = id_artista
                    artista_actual = str(id_artista)
                    artista = [str(artista_actual)] + fragmentos[2:4]
                    artistas.append('\t'.join(artista))
                    id_artista += 1

                # Veamos si ya existe la cancion
                cancion = canciones_guardadas.get(fragmentos[4], False)
                if cancion:
                    cancion_actual = str(cancion)
                else:
                    canciones_guardadas[fragmentos[4]] = id_cancion
                    cancion_actual = id_cancion
                    cancion = [str(cancion_actual)] + fragmentos[4:5]
                    # Quitamos el \n del final, y añadimos el id del artista
                    cancion += [fragmentos[5][:-1], f'{artista_actual}\n']
                    canciones.append(
                                    '\t'.join(cancion)
                                    )
                    id_cancion += 1

                # Convirtamos la fecha segun nos convenga (sino es nula)
                if fragmentos[1]:
                    fecha = dt.strptime(fragmentos[1], '%Y-%m-%dT%H:%M:%SZ')
                    fecha = fecha.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    fecha = fragmentos[1]
                escucha = [
                            str(i), fragmentos[0][-6:],
                            str(cancion_actual), f'{fecha}\n'
                        ]
                escuchas.append('\t'.join(escucha))
                i += 1

    with open(f'{path}/escuchas_filtrado.tsv', 'w', encoding='utf-8') as file:
        file.writelines(escuchas)

    with open(f'{path}/artistas_filtrado.tsv', 'w', encoding='utf-8') as file:
        file.writelines(artistas)

    with open(f'{path}/canciones_filtrado.tsv', 'w', encoding='utf-8') as file:
        file.writelines(canciones)

    return escuchas, canciones, artistas


def formatear_usuarios(path: str):
    '''
    Formateará el id del usuario
    Args:
        path: ubicación del fichero "userid-profile.tsv"
    '''

    with open(f'{path}/userid-profile.tsv', 'r', encoding='utf-8') as fichero:
        # Reutilizamos la cabezera
        usuarios = [fichero.readline()]

        linea = fichero.readline()
        while linea:
            cadena = linea.split('\t')
            cadena[0] = cadena[0][5:]
            # Convertimos al formato que usaremos en MySQL
            if cadena[-1][:-1]:
                time = dt.strptime(cadena[-1][:-1], '%b %d, %Y')
                cadena[-1] = time.strftime('%Y-%m-%d')
            usuarios.append('\t'.join(cadena) + '\n')
            linea = fichero.readline()

    with open(f'{path}/usuarios_filtrado.tsv', 'w', encoding='utf-8') as file:
        file.writelines(usuarios)

    return usuarios


def main(data_path: str):
    '''
    Filtrara los datos necesarios cada vez que se ejecute el programa
    Además, guardará los archivos para futuras ejecuciones

    Args:
        data_path: ubicación de los ficheros "userid-profile.tsv" y
        "userid-timestamp-artid-artname-traid-traname.tsv"

    Returns:
        escuchas: lista con cada escucha
        canciones: lista con las canciones
        artistas: lista con los artistas
        usuarios: lista con los usuarios registrados
    '''

    escuchas, canciones, artistas = filtrar_escuchas(data_path)

    return escuchas, canciones, artistas, formatear_usuarios(data_path)
