package routines;

import sapphire.util.DataSet;

public class Main {
    public static void main(String[] args) {

        String[] nextRecord;

        //String pathFile = "C:\\Thin Film Tensile 2_1.csv";
        String pathFile = "C:\\Fiber Tensile 1_1.csv";
        DataSet dataSet = ImportDynamicDataCVS.processRawData(pathFile);
    }

}