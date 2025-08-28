# Big-data-Wordcount

Se aplico wordcount e indice invertido en un archivo de 20gb de 129 M de lineas y aprox 3 Billones de palabras:
<img width="1040" height="230" alt="image" src="https://github.com/user-attachments/assets/1d86b299-be8f-412a-a3a5-7770dc60e0e8" />
El wordcount de forma estandar por chunks y parser
El indice invertido se aplica por cada 400,000 lineas en el documento tomandolo como un documento numerado, el resultado se divide en {palabra,nro de documento,cantidad}
