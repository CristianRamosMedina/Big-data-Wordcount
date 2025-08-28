# Big-data-Wordcount

Se aplico wordcount e indice invertido en un archivo de 20gb de 129 M de lineas y aprox 3 Billones de palabras:
<img width="1038" height="232" alt="image" src="https://github.com/user-attachments/assets/54d50ad9-3727-4321-afd3-0ac08df08742" />
El documento fue creado con Faker en paralelo mediante bloques y archivos temporales
El wordcount de forma estandar por chunks y parser
El indice invertido se aplica por cada 400,000 lineas en el documento tomandolo como un documento numerado, el resultado se divide en {palabra,nro de documento,cantidad}
