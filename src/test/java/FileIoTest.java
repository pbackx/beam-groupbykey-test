import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import test.Employee;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import javax.validation.constraints.NotNull;

public class FileIoTest {
    private static final Random RND = new Random();
    public static final AvroCoder<Employee> EMPLOYEE_AVRO_CODER = AvroCoder.of(Employee.class, Employee.getClassSchema());

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Pipeline pipeline;

    @Before
    public void setUp() {
        final SparkContextOptions options = PipelineOptionsFactory.create().as(SparkContextOptions.class);
        options.setProvidedSparkContext(new JavaSparkContext(getSparkConf()));
        options.setUsesProvidedSparkContext(true);
        options.setStorageLevel("MEMORY_AND_DISK");
        options.setRunner(SparkRunner.class);
        pipeline = Pipeline.create(options);
    }

    @Test
    public void test() throws IOException {
        final File inputFolder = folder.newFolder("input");
        final File outputFolder = folder.newFolder("output");

        for (int i = 0; i < 50; i++) {
            pipeline.apply("Create test data",
                           Create.of(testData())
                                 .withCoder(EMPLOYEE_AVRO_CODER))
                    .apply(AvroIO.write(Employee.class)
                                 .to(String.format("%s/%s-%d-",
                                                   inputFolder.getAbsolutePath(),
                                                   "input-avro",
                                                   i)));

            pipeline.run().waitUntilFinish();
        }
        System.out.println("Data generated.");

        PCollection<KV<Integer, Employee>> joined =
                pipeline.apply(Create.empty(KvCoder.of(VarIntCoder.of(), EMPLOYEE_AVRO_CODER)));

        for (int i = 0; i < 50; i++) {
            final PCollection<KV<Integer, Employee>> employees =
                    pipeline.apply(AvroIO.read(Employee.class)
                                         .from(inputFolder.getAbsolutePath() + "/input-avro-" + i + "-*"))
                            .apply("Add Id", WithKeys.of(Employee::getId))
                            .setCoder(KvCoder.of(VarIntCoder.of(), EMPLOYEE_AVRO_CODER));

            joined = Join.innerJoin("join " + i, joined, employees)
                         .apply("Back to single employee", ParDo.of((new FlattenEmployee())));
        }

        joined.apply("count", Count.perKey())
              .apply("to text", MapElements.via(new ToString()))
              .apply("output", TextIO.write()
                                     .to(outputFolder.getAbsolutePath() + "/count-")
                                     .withNumShards(1));
        pipeline.run().waitUntilFinish();

        System.out.println("Done.");
    }

    private Iterable<Employee> testData() {
        return () -> new Iterator<Employee>() {
            private int count = 0;

            @Override
            public boolean hasNext() {
                return count < 1_000;
            }

            @Override
            public Employee next() {
                count++;
                final Employee employee = new Employee();
                employee.setId(RND.nextInt(1000));
                employee.setRelation(RND.nextInt(1000));
                employee.setAge(RND.nextInt(120));
                employee.setName(RandomStringUtils.randomAlphabetic(50));
                return employee;
            }
        };
    }

    @NotNull
    private static SparkConf getSparkConf() {
        final SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.master", "local[7]");
        sparkConf.set("spark.app.name", "FileIO loadtest");
        sparkConf.set("spark.eventLog.enabled", "true");
        return sparkConf;
    }

    private static class FlattenEmployee extends DoFn<KV<Integer, KV<Employee, Employee>>, KV<Integer, Employee>> {
        @ProcessElement
        public void process(@Element KV<Integer, KV<Employee, Employee>> input, OutputReceiver<KV<Integer, Employee>> output) {
            output.output(KV.of(input.getKey(), input.getValue().getKey()));
            output.output(KV.of(input.getKey(), input.getValue().getValue()));
        }
    }

    private static class ToString extends SimpleFunction<KV<Integer, Long>, String> {
        @Override
        public String apply(KV<Integer, Long> input) {
            return String.format("%d: %d", input.getKey(), input.getValue());
        }
    }
}
