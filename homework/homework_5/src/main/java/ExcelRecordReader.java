import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.io.IOException;
import java.util.Iterator;

public class ExcelRecordReader extends RecordReader<Text, Text> {
    private Text key = new Text();
    private Text value = new Text();

    private XSSFWorkbook workbook;
    private Iterator<Row> rowIterator;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        Path path = fileSplit.getPath();
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        try (FSDataInputStream fileIn = fs.open(path)) {
            workbook = new XSSFWorkbook(fileIn);
            XSSFSheet sheet = workbook.getSheetAt(0); 
            rowIterator = sheet.iterator();
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!rowIterator.hasNext()) {
            return false;
        }

        Row row = rowIterator.next();
        StringBuilder rowData = new StringBuilder();

        for (Cell cell : row) {
            boolean FirstCell=true;
            String cellValue=new String();
            switch (cell.getCellType()) {
                case STRING:
                    cellValue=cell.getStringCellValue();
                    break;
                case NUMERIC:
                    cellValue=Double.toString(cell.getNumericCellValue());
                    break;
            }
            if(FirstCell){
                key.set(cellValue);
                FirstCell=false;
            }
            else{
                rowData.append(cellValue);
                rowData.append("/t");
            }
        }

        value.set(rowData.toString());

        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        if (workbook != null) {
            workbook.close();
        }
    }
}
