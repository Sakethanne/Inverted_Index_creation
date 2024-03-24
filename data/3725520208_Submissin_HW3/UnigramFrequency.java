import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;

public class ModifiedUnigramFrequency {

  public static class ModifiedUnigramInvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

    private final Text wordKey = new Text();
    private final Text docIdValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] parts = value.toString().split("\\t", 2);

      docIdValue.set(parts[0]);

      String wordTokensSet = parts[1].replaceAll("[^a-zA-Z]+", " ").toLowerCase(Locale.ROOT);
      StringTokenizer tokenizer = new StringTokenizer(wordTokensSet, " ");
      while (tokenizer.hasMoreTokens()) {
        String wordToken = tokenizer.nextToken();
        if (!wordToken.trim().isEmpty()) {
          wordKey.set(wordToken);
          context.write(wordKey, docIdValue);
        }
      }
    }
  }

  public static class ModifiedUnigramInvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text wordKey, Iterable<Text> docIdValues, Context context)
        throws IOException, InterruptedException {
      Map<String, Integer> docIdToCountMap = new HashMap<>();
      for (Text docId : docIdValues) {
        String docIdString = docId.toString();
        docIdToCountMap.put(docIdString, docIdToCountMap.getOrDefault(docIdString, 0) + 1);
      }

      StringBuilder docIdFrequencies = new StringBuilder();
      for (Map.Entry<String, Integer> entry : docIdToCountMap.entrySet()) {
        if (docIdFrequencies.length() > 0) {
          docIdFrequencies.append("\t");
        }
        String docId = entry.getKey();
        Integer frequency = entry.getValue();
        String docIdFrequency = String.format("%s:%d", docId, frequency);
        docIdFrequencies.append(docIdFrequency);
      }

      context.write(wordKey, new Text(docIdFrequencies.toString()));
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    if (args.length != 2) {
      System.err.println("Usage: Unigram Inverted Index <input path> <output path>");
      System.exit(-1);
    }
    String inputFile = args[0];
    String outputFile = args[1];

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Unigram Inverted Index");
    job.setJarByClass(ModifiedUnigramFrequency.class);
    job.setMapperClass(ModifiedUnigramInvertedIndexMapper.class);
    job.setReducerClass(ModifiedUnigramInvertedIndexReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    Path inputFilePath = new Path(inputFile);
    Path outputFilePath = new Path(outputFile);
    FileSystem fileSystem = outputFilePath.getFileSystem(conf);
    if (fileSystem.exists(outputFilePath)) {
      fileSystem.delete(outputFilePath, true);
    }
    FileInputFormat.addInputPath(job, inputFilePath);
    FileOutputFormat.setOutputPath(job, outputFilePath);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}