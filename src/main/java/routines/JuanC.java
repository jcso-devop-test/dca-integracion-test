package routines;

import com.opencsv.CSVReader;
import sapphire.SapphireException;
import sapphire.util.DataSet;
import labvantage.limsci.lvws.WSProvider;


import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;


public class JuanC {

    private final static String DRIVER_NAME = "INSTRON TENSIL";
    public static WSProvider utils = null;


    public static DataSet processRawData(String filePath, String instrumentId, String instConnectionId, String connectionId) {

        //String path = "D:\\DCA\\Proyectos\\Talend\\test.csv";
        String[][] firstTable;
        String[][] secondTable;

        try ( CSVReader reader = new CSVReader(new FileReader(filePath))) {

            String[] firstRowFile = reader.readNext();
            String firstColumnFirstRow = firstRowFile[0];

            if (firstColumnFirstRow.contains("Sample file name")) {
                firstTable = getFirstTable(reader, firstRowFile);

                String[] firstRowSecondTable = reader.readNext();
                String firstColumnSecondTable = firstRowSecondTable[0];

                if (firstColumnSecondTable.contains("Results Table 1")) {
                    secondTable = getSecondTable(getSecondTableList(reader), firstRowFile.length);
                    return getDataSet(getNewTable(firstTable, secondTable));
                } else {
                    throw new SapphireException(DRIVER_NAME  +":::ERROR:: the first column of the second table is invalid, column name found '" + firstColumnSecondTable + "', is aborted");
                }
            } else {
                throw new SapphireException(DRIVER_NAME  +":::ERROR:: the first column of the file is invalid, column name found '" + firstColumnFirstRow + "', is aborted");
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException | SapphireException e) {
            throw new RuntimeException(e);
        }

    }

    private static String[][] getFirstTable (CSVReader reader, String[] firstRow) {
        String nameMethod = "getFirstTable";
        System.out.println(DRIVER_NAME + ": " + nameMethod + " - Start ");

        int countColumns = firstRow.length;
        String[][] firstTable = new String[3][countColumns];

        for (int f = 0; f<3; f++) {
            for (int c = 0; c<countColumns; c++) {
                firstTable[f][c] = firstRow[c];
            }
            try {
                firstRow = reader.readNext();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        System.out.println(DRIVER_NAME + ": " + nameMethod + " - End ");
        return firstTable;
    }

    private static ArrayList<String[]> getSecondTableList (CSVReader reader) {
        String nameMethod = "getSecondTableList";
        System.out.println(DRIVER_NAME + ": " + nameMethod + " - Start ");

        ArrayList<String[]> secondTable = new ArrayList<>();
        String[] nextRecord;

        try {
            while ((nextRecord = reader.readNext()) != null) {
                secondTable.add(nextRecord);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        System.out.println(DRIVER_NAME + ": " + nameMethod + " - End ");
        return secondTable;
    }

    private static String[][] getSecondTable (ArrayList<String[]> secondTableList, int countColumns) {
        String nameMethod = "getSecondTable";
        System.out.println(DRIVER_NAME + ": " + nameMethod + " - Start ");


        int countRows = secondTableList.size();
        String[][] secondTable = new String[countRows][countColumns];

        for (int f=0; f<countRows; f++) {
            for (int c=0; c<countColumns; c++) {
                secondTable[f][c] = secondTableList.get(f)[c];
            }
        }

        System.out.println(DRIVER_NAME + ": " + nameMethod + " - End ");
        return secondTable;
    }

    private static ArrayList<String[]> getNewTable (String[][] firstTable, String[][] secondTable) {

        ArrayList<String[]> newTable = new ArrayList<>();
        String[] row = new String[5];

        ArrayList<String> instrumentFieldFirstTable = new ArrayList<>();
        ArrayList<String> unitsFirstTable = new ArrayList<>();
        ArrayList<String> valuesItemFirstTable = new ArrayList<>();

        String valueFirstTable;

        for (int f = 0; f < firstTable.length; f++) {
            for (int c=1; c < firstTable[f].length; c++) {
                valueFirstTable = firstTable[f][c];
                if (f==0 && valueFirstTable.isEmpty()) {
                    break;
                }
                switch (f) {
                    case 0:
                        instrumentFieldFirstTable.add(valueFirstTable);
                        break;
                    case 1:
                        unitsFirstTable.add(valueFirstTable);
                        break;
                    case 2:
                        valuesItemFirstTable.add(valueFirstTable);
                        break;
                    default:
                        break;
                }
            }
        }

        ArrayList<String> instrumentFieldSecondTable = new ArrayList<>();
        ArrayList<String> unitsSecondTable = new ArrayList<>();
        ArrayList<String> valuesItemSecondTable = new ArrayList<>();
        ArrayList<String[]> valuesRowListSecondTable = new ArrayList<>();

        String valueSecondTable;
        int positionReplicate = -1;

        for (int f=0; f < secondTable.length; f++) {
            for (int c=1; c<secondTable[f].length; c++) {
                valueSecondTable = secondTable[f][c];
                if ( f == 0 && valueSecondTable.contains("LIMS ID")) {
                    continue;
                }
                if (valueSecondTable.contains("Replicate") || valueSecondTable.contains("dataset")) {
                    positionReplicate = c;
                }
                if (f == 0 && c>=2){
                    instrumentFieldSecondTable.add(valueSecondTable);
                } else if (f == 1 && c>=2) {
                    unitsSecondTable.add(valueSecondTable);
                } else if (f >= 2 && c>=1) {
                    valuesItemSecondTable.add(valueSecondTable);
                }
            }
            if (f>=2) {
                int countColumnsValue = valuesItemSecondTable.size();
                String[] rowValues = new String[countColumnsValue];
                for (int i=0; i<countColumnsValue; i++) {
                    rowValues[i] = valuesItemSecondTable.get(i);
                }
                valuesRowListSecondTable.add(rowValues);
                valuesItemSecondTable.clear();
            }
        }

        int countInstrumentFieldSecondTable = instrumentFieldSecondTable.size();
        int countInstrumentFieldFirstTable = instrumentFieldFirstTable.size();
        int sizeRows = valuesRowListSecondTable.size();

        for (int v=0; v<sizeRows; v++) {

            for (int i=0; i<countInstrumentFieldFirstTable; i++) {
                row[0] = valuesRowListSecondTable.get(v)[0];
                if (positionReplicate == -1) {
                    row[1] = "1";
                } else {
                    row[1] = valuesRowListSecondTable.get(v)[positionReplicate-1];
                }
                row[2] = instrumentFieldFirstTable.get(i);
                row[3] = valuesItemFirstTable.get(i);
                row[4] = unitsFirstTable.get(i);
                newTable.add(row);
                row = new String[5];
            }

            for (int i=0; i<countInstrumentFieldSecondTable; i++) {
                row[0] = valuesRowListSecondTable.get(v)[0];
                if (positionReplicate == -1) {
                    row[1] = "1";
                } else {
                    row[1] = valuesRowListSecondTable.get(v)[positionReplicate-1];
                }

                row[2] = instrumentFieldSecondTable.get(i);
                row[3] = valuesRowListSecondTable.get(v)[i+1];
                row[4] = unitsSecondTable.get(i);
                newTable.add(row);
                row = new String[5];
            }
        }

        return newTable;
    }

    private static DataSet getDataSet (ArrayList<String[]> table) {
        DataSet dataSet = new DataSet();

        dataSet.addColumn("sdcid", 0);
        dataSet.addColumn("keyid1", 0);
        dataSet.addColumn("dataset", 0);
        dataSet.addColumn("instrumentfield", 0);
        dataSet.addColumn("value", 0);
        dataSet.addColumn("units", 0);
        int r ;
        for (int i=0; i<table.size(); i++) {
            r=dataSet.addRow();
            dataSet.setValue(r, "sdcid", "sample");
            dataSet.setValue(r, "keyid1", table.get(i)[0]);
            dataSet.setValue(r, "dataset", table.get(i)[1]);
            dataSet.setValue(r, "instrumentfield", table.get(i)[2]);
            dataSet.setValue(r, "value", table.get(i)[3]);
            dataSet.setValue(r, "units", table.get(i)[4]);
        }

        return dataSet;
    }
}
