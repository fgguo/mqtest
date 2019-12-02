/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.openmessaging.benchmark;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import javax.sound.midi.Soundbank;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.ls.LSOutput;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.openmessaging.benchmark.worker.DistributedWorkersEnsemble;
import io.openmessaging.benchmark.worker.LocalWorker;
import io.openmessaging.benchmark.worker.Worker;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.openmessaging.benchmark.driver.rabbitmq.*;
public class Benchmark {

    static class Arguments {

        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;

        @Parameter(names = { "-d",
                "--drivers" }, description = "Drivers list. eg.: pulsar/pulsar.yaml,kafka/kafka.yaml", required = true)
        public List<String> drivers;

        @Parameter(names = { "-w",
                "--workers" }, description = "List of worker nodes. eg: http://1.2.3.4:8080,http://4.5.6.7:8080")
        public List<String> workers;

        @Parameter(names = { "-wf",
                "--workers-file" }, description = "Path to a YAML file containing the list of workers addresses")
        public File workersFile;

        @Parameter(description = "Workloads", required = true)
        public List<String> workloads;
    }

    public static void main(String[] args) throws Exception {
        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("messaging-benchmark");

//        try {
//            jc.parse(args);
//        } catch (ParameterException e) {
//            System.err.println(e.getMessage());
//            jc.usage();
//            System.exit(-1);
//        }
//
//        if (arguments.help) {
//            jc.usage();
//            System.exit(-1);
//        }

//        if (arguments.workers != null && arguments.workersFile != null) {
//            System.err.println("Only one between --workers and --workers-file can be specified");
//            System.exit(-1);
//        }
        List<String> workloads_local = new ArrayList<String>();
        List<String> drivers_local = new ArrayList<String>();
        System.out.println("***********************choose message queue to test************************");
        System.out.println("input 1 for kafka");
        System.out.println("input 2 for pulsar");
        System.out.println("input 3 for rabbitMQ");
        System.out.println("input 4 for rocketMQ");
        System.out.println("input 5 for activeMQ");
        Scanner scanner = new Scanner(System.in);
        int mq = scanner.nextInt(); 
        switch (mq) {
		case 1:
			workloads_local.add("./src/main/java/workloads/1-topic-1-partition-1kb.yaml");
		    drivers_local.add("./src/main/java/driver-yaml/kafka.yaml");
			break;
		case 2:
			
			break;
		case 3:
			workloads_local.add("./src/main/java/workloads/1-topic-1-partition-100b.yaml");
		    drivers_local.add("./src/main/java/driver-yaml/rabbitmq.yaml");
		    
			break;
		case 4:
	
			break;
		case 5:
			
			break;
		default:
			break;
		}
        
        arguments.workloads = workloads_local;
        arguments.drivers = drivers_local;
        if (arguments.workers == null && arguments.workersFile == null) {
            File defaultFile = new File("workers.yaml");
            if (defaultFile.exists()) {
                log.info("Using default worker file workers.yaml");
                arguments.workersFile = defaultFile;
            }
        }

        if (arguments.workersFile != null) {
            log.info("Reading workers list from {}", arguments.workersFile);
            arguments.workers = mapper.readValue(arguments.workersFile, Workers.class).workers;
        }

        // Dump configuration variables
        log.info("Starting benchmark with config: {}", writer.writeValueAsString(arguments));

        Map<String, Workload> workloads = new TreeMap<>();
        for (String path : arguments.workloads) {
            File file = new File(path);
            System.out.println(path);
            String name = file.getName().substring(0, file.getName().lastIndexOf('.'));

            workloads.put(name, mapper.readValue(file, Workload.class));
        }

        log.info("Workloads: {}", writer.writeValueAsString(workloads));
        //System.out.println(drivers_local.get(0));
        Worker worker;

//        if (arguments.workers != null && !arguments.workers.isEmpty()) {
//            worker = new DistributedWorkersEnsemble(arguments.workers);
//        } else {
            // Use local worker implementation
            worker = new LocalWorker();
        //}

        workloads.forEach((workloadName, workload) -> {
            arguments.drivers.forEach(driverConfig -> {
                try {
                    File driverConfigFile = new File(driverConfig);
                    DriverConfiguration driverConfiguration = mapper.readValue(driverConfigFile,
                            DriverConfiguration.class);
                    log.info("--------------- WORKLOAD : {} --- DRIVER : {}---------------", workload.name,
                            driverConfiguration.name);

                    // Stop any left over workload
                    worker.stopAll();

                    worker.initializeDriver(new File(driverConfig));

                    WorkloadGenerator generator = new WorkloadGenerator(driverConfiguration.name, workload, worker);

                    TestResult result = generator.run();

                    String fileName = String.format("%s-%s-%s.json", workloadName, driverConfiguration.name,
                            dateFormat.format(new Date()));

                    log.info("Writing test result into {}", fileName);
                    writer.writeValue(new File(fileName), result);

                    generator.close();
                } catch (Exception e) {
                    log.error("Failed to run the workload '{}' for driver '{}'", workload.name, driverConfig, e);
                } finally {
                    try {
                        worker.stopAll();
                    } catch (IOException e) {
                    }
                }
            });
        });

        worker.close();
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");

    private static final Logger log = LoggerFactory.getLogger(Benchmark.class);
}
