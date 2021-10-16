import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Log1 {

    public class TokenMapper extends Mapper<Object, Text,Text, IntWritable>{

    }
}
