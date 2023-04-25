from pyspark import SparkContext
import sys
import string
import random

class leeFichero():    
    def __init__(self, fichero):
        self.fichero = fichero
        self.salidaCuentaPalabras = ''
        self.salidaLineasAleatorias = ''
    
    @staticmethod
    def word_split(line):
        for c in string.punctuation+"¿!«»":
            line = line.replace(c,' ')
            line = line.lower()
        return len(line.split())
    
    """
    Método para copiar de forma aleatoria ciertas líneas de un fichero en otro
    Si no se da nombre de fichero se usa el de la clase, si se le da, se cambia el de la clase.
    El fichero de salida es de la forma nombreFicheroOriginal_s05.txt.
    """
    def copiaLineasAleatorias(self, fichero = ''):
        if fichero != '':
            self.setFichero(fichero)
        self.salidaLineasAleatorias = self.fichero[0:-4]+'_s05.txt'
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            data = sc.textFile(self.fichero)
            rdd  = data.map(lambda x: [x, random.randint(0, 1)]).filter(lambda x : x[1]==1).map(lambda x: x[0])
            with open(self.salidaLineasAleatorias, 'w') as f:
                for r in rdd.collect():
                    f.write(r + '\n')
                
    def getFicherosSalida(self):
        return {'lineasAleatorias': self.salidaLineasAleatorias, 'cuentaPalabras': self.salidaCuentaPalabras}
          
    def setFichero(self, fichero):
        self.fichero = fichero
    
    """
    Método para contar las líneas de un fichero y escribir ese número en otro
    Si no se da nombre de fichero se usa el de la clase, si se le da, se cambia el de la clase.
    El fichero de salida es de la forma out_nombreFicheroOriginal.txt.
    """
    def cuentaPalabras(self, fichero = ''):
        if fichero != '':
            self.setFichero(fichero)
        self.salidaCuentaPalabras = 'out_' + self.fichero
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            data = sc.textFile(self.fichero)
            rdd = data.map(lambda x: leeFichero.word_split(x))
            mensaje = f'Numero de palabras en {self.fichero} es :{rdd.sum()}'
            with open(self.salidaCuentaPalabras, 'w') as f:
                f.write(mensaje)
    

def main(fichero = 'quijote.txt', resume = False):   
    f = leeFichero(fichero)   
    if resume:
        f.copiaLineasAleatorias() 
        f.setFichero(f.getFicherosSalida()['lineasAleatorias'])
    f.cuentaPalabras()

if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(sys.argv[1])
    elif len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2]=='resume')
    else:
        main()
        
