package routines;

import sapphire.util.DataSet;

public class Main {
    public static void main(String[] args) {

        String[] nextRecord;

        //String pathFile = "C:\\Thin Film Tensile 2_1.csv"; Thin Film Tensile 2_1.csv
        String pathFile = "C:\\Heinsohn\\Personal\\Documentos DCA\\Integracion\\Thin Film Tensile 2_1.csv";
        DataSet dataSet = ImportInstronTensile.processRawData(pathFile, "","",""); //ImportDynamicDataCVS.processRawData(pathFile);
    }

}