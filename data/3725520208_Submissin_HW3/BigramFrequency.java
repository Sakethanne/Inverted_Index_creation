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
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class BigramsFrequency {

  public static class BigramsInvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

    private final Text bigramKey = new Text();
    private final Text docIdValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] splitDoc = value.toString().split("\\t", 2);

      docIdValue.set(splitDoc[0]);

      String wordTokensSet = splitDoc[1].replaceAll("[^a-zA-Z]+", " ").toLowerCase(Locale.ROOT);
      StringTokenizer tokenizer = new StringTokenizer(wordTokensSet, " ");
      String firstWord = null;
      String secondWord = null;
      List<String> bigramsList = new ArrayList<String>(5);
      bigramsList.add("bruce willis");
      bigramsList.add("computer science");
      bigramsList.add("information retrieval");
      bigramsList.add("los angeles");
      bigramsList.add("power politics");
      while (tokenizer.hasMoreTokens()) {
        String wordToken = tokenizer.nextToken();
        if (!wordToken.trim().isEmpty()) {
          if (firstWord == null) {
            firstWord = wordToken;
            continue;
          } else if (secondWord == null) {
            secondWord = wordToken;
          } else {
            firstWord = secondWord;
            secondWord = wordToken;
          }
          StringBuilder bigramWord = new StringBuilder();
          bigramWord.append(firstWord);
          bigramWord.append(" ");
          bigramWord.append(secondWord);
          String bigramString = bigramWord.toString();
          if (bigramsList.contains(bigramString)) {
            bigramKey.set(bigramString);
            context.write(bigramKey, docIdValue);
          }
        }
      }
    }
  }

  public static class BigramsInvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text bigramKey, Iterable<Text> docIdValues, Context context)
        throws IOException, InterruptedException {
      Map<String, Integer> docIdToCountMap = new HashMap<>();
      for (Text docId : docIdValues) {
        String docIdString = docId.toString();
        docIdToCountMap.put(docIdString,
            docIdToCountMap.getOrDefault(docIdString, 0) + 1);
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

      context.write(bigramKey, new Text(docIdFrequencies.toString()));
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    if (args.length != 2) {
      System.err.println("Usage: Bigrams Inverted Index <input path> <output path>");
      System.exit(-1);
    }
    String inputFile = args[0];
    String outputFile = args[1];

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Bigrams Inverted Index");
    job.setJarByClass(BigramsFrequency.class);
    job.setMapperClass(BigramsInvertedIndexMapper.class);
    job.setReducerClass(BigramsInvertedIndexReducer.class);
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