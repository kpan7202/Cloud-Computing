package com.flink;

import java.util.*;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class FlinkTask {

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		String samples = params.getRequired("samples");
		String output = params.getRequired("output");
		if (!output.substring(output.length() - 1 ).equals("/")) {
			output += "/";
		}
		
		final int runTask = params.getInt("task", 1);
		final int k = params.getInt("k", 5);
		final int iteration = params.getInt("iteration", 10);
		int dimensions = params.getInt("dimensions", 3);
		dimensions = dimensions < 3 ? 3 : (dimensions > 14 ? 14 : dimensions);
		final int d = dimensions;
		
		// read sample input from file, get valid data 
		DataSet<Tuple3<String, List<Double>, Integer>> sampleDS = env.readTextFile(samples)
			.flatMap( (line, out) -> {
				try {
					String [] str = line.split(",");						
					int fsca = Integer.parseInt(str[1]);
					int ssca = Integer.parseInt(str[2]);
					if (fsca <= 150000 && fsca >= 1 && ssca <= 150000 && ssca >= 1) {
						List<Double> values = new ArrayList<Double>();
						if (d == 3) {
							values.add(Double.parseDouble(str[11])); //Ly6C
							values.add(Double.parseDouble(str[7])); //CD11b
							values.add(Double.parseDouble(str[6])); //SCA1
						} 
						else {
							for(int i = 3; i < 3 + d; i++) {
								values.add(Double.parseDouble(str[i]));
							}
						}
						out.collect(new Tuple3<String, List<Double>, Integer>(str[0], values, 1));
					}
				} catch (Exception e) {}
			});
		
		if (runTask == 1) {
			String metadata = params.getRequired("metadata");
			// read metadata from file
			DataSet<Tuple2<String, String>> sampleResearchers =
		            env.readTextFile(metadata)
		                .flatMap((line, out) -> {
		                    String[] values = line.split(",");
	
		                    if(values.length >= 8) {
		                        String id = values[0];
		                        String[] researchers = values[values.length - 1].split(";");
	
		                        for(String name : researchers) {
		                            out.collect(new Tuple2<String, String>(id, name.trim()));
		                        }
		                    }
		                });
			
			DataSet<Tuple2<String, Integer>> joinSampleResearchers =
					sampleResearchers
	                    .join(sampleDS)
	                    .where(0)
	                    .equalTo(0)
	                    .projectFirst(1)
	                    .projectSecond(2);
			
			DataSet<Tuple2<String, Integer>> countSampleResearchers = 
					joinSampleResearchers
		                  .groupBy(0)
		                  .sortGroup(0, Order.ASCENDING)
		                  .reduceGroup(new ReduceSampleResearchers());
			
			countSampleResearchers.sortPartition(1, Order.DESCENDING).setParallelism(1).writeAsCsv(output + "1", "\n", "\t");
		}
		
		// task 2
		if (runTask == 2) {
			final String centroid = params.get("centroid", "");
			DataSet<Tuple3<String, List<Double>, Integer>> centroidDS = getCentroid(centroid, k, dimensions, env);
			// start k means iteration
			DataSet<Tuple3<String, List<Double>, Integer>> finalCentroidDS = kMeans(centroidDS, sampleDS, iteration);
			
			// sort & write to 1 file
			finalCentroidDS.sortPartition(0, Order.ASCENDING).setParallelism(1).map(c -> {
				String result = c.f0 + "\t" + c.f2;
				for (Double feature: c.f1) {
					result += "\t" + String.valueOf(feature);
				}
				return new Tuple1<String>(result);
			}).writeAsCsv(output + "2");
					
		}
		// task 3
		if (runTask == 3) {
			final String centroid = params.getRequired("centroid");
			DataSet<Tuple3<String, List<Double>, Integer>> centroidDS = getCentroid(centroid, k, dimensions, env);
			// set the centroid id for each data point based on minimum distance
			DataSet<Tuple4<String, List<Double>,Double,Integer>> finalSampleDS = sampleDS
					.map(new RichMapFunction<Tuple3<String, List<Double>, Integer>, Tuple4<String, List<Double>,Double,Integer>>(){
						private List<Tuple3<String, List<Double>, Integer>> centroids = new ArrayList<Tuple3<String, List<Double>, Integer>>();
						@Override
						public void open(Configuration parameters) throws Exception {
							this.centroids = getRuntimeContext().getBroadcastVariable("finalCentroids");
						}
						
						@Override
						public Tuple4<String, List<Double>,Double,Integer> map(Tuple3<String, List<Double>, Integer> row) throws Exception {
							List<Double> features = row.f1;
							String pos = "0";
							int numSamples = 0;
							double minDist = Double.MAX_VALUE;
							for (int i = 0; i < this.centroids.size(); i++) {
								List<Double> centroidFeatures = this.centroids.get(i).f1;
								double eucDist = 0;									
								for (int j = 0; j < centroidFeatures.size(); j++) {
									eucDist += Math.pow(features.get(j) - centroidFeatures.get(j), 2);
								}
								eucDist = Math.sqrt(eucDist);
								
								if (eucDist < minDist) {
									pos = this.centroids.get(i).f0;
									minDist = eucDist;
									numSamples = this.centroids.get(i).f2;
								}
							}
							return new Tuple4<String, List<Double>,Double,Integer>(pos, features, minDist,numSamples);
						}
							
					}).withBroadcastSet(centroidDS, "finalCentroids");
			
			// remove 10% data for each centroid
			DataSet<Tuple3<String, List<Double>, Integer>> filteredSampleDS = finalSampleDS
					.groupBy(0)
					.sortGroup(2, Order.DESCENDING)
					.reduceGroup((in, out) ->{
						int ctr = 0;
						for(Tuple4<String, List<Double>,Double,Integer> tuple : in) {								
							if (ctr + 1 > Math.floor(tuple.f3 * 0.1)) {
								out.collect(new Tuple3<String, List<Double>, Integer>(tuple.f0, tuple.f1, 1));
							}
							else {
								ctr++;
							}
						}
					});
			
			// do another k means
			DataSet<Tuple3<String, List<Double>, Integer>> mostFinalCentroidDS = kMeans(centroidDS, filteredSampleDS, iteration);
			
			// sort & write to 1 file
			mostFinalCentroidDS.sortPartition(0, Order.ASCENDING).setParallelism(1).map(c -> {
				String result = c.f0 + "\t" + c.f2;
				for (Double feature: c.f1) {
					result += "\t" + String.valueOf(feature);
				}
				return new Tuple1<String>(result);
			}).writeAsCsv(output + "3");
			
		}
		
		env.execute();
	}
	
	public static DataSet<Tuple3<String, List<Double>, Integer>> getCentroid(String centroidLocation, int k, int dimensions, final ExecutionEnvironment env) {
		DataSource<String> centroidString = null;
		
		if (!centroidLocation.equals("")) { // if given centroid file
			centroidString = env.readTextFile(centroidLocation);
		}
		else { // random the point between 0-1
			List<String> centroids = new ArrayList<String>();
			for (int i = 1; i <= k; i++) {
				String c = String.valueOf(i) + "\t0";
				for (int j = 0; j < dimensions; j++) {
					c += "\t" + Math.random();
				}
				centroids.add(c);
			}
			
			centroidString = env.fromCollection(centroids);
					
		}
		
		DataSet<Tuple3<String, List<Double>, Integer>> centroidDS = centroidString.map(line -> {
			String [] str = line.split("\t");
			List<Double> values = new ArrayList<Double>();
			for (int i = 2; i < str.length; i++) {
				values.add(Double.valueOf(str[i]));
			}
			return new Tuple3<String, List<Double>, Integer>(str[0], values, Integer.parseInt(str[1]));
		});
		
		return centroidDS;
	}
	
	public static DataSet<Tuple3<String, List<Double>, Integer>> kMeans(DataSet<Tuple3<String, List<Double>, Integer>> centroidDS, 
			DataSet<Tuple3<String, List<Double>, Integer>> sampleDS, final int iteration) {
		IterativeDataSet<Tuple3<String, List<Double>, Integer>> iterDS = centroidDS.iterate(iteration);
		
		DataSet<Tuple3<String, List<Double>, Integer>> updatedSampleDS = sampleDS
				.map(new MapSampleData()).withBroadcastSet(iterDS, "centroids");
		
		DataSet<Tuple3<String, List<Double>, Integer>> updatedCentroids = updatedSampleDS.groupBy(0)
				.reduce((c1, c2) -> {
					List<Double> features1 = c1.f1;
					List<Double> features2 = c2.f1;
					List<Double> sumFeatures = new ArrayList<Double>();
					for (int i = 0; i < features1.size(); i++) {
						sumFeatures.add(features1.get(i) + features2.get(i));
					}
					return new Tuple3<String, List<Double>, Integer>(c1.f0, sumFeatures, c1.f2 + c2.f2);
				})
				.map(c -> {
					List<Double> features = c.f1;
					List<Double> meanfeatures = new ArrayList<Double>();
					for (Double feature : features) {
						meanfeatures.add(feature / c.f2);
					}
					return new Tuple3<String, List<Double>, Integer>(c.f0, meanfeatures, c.f2);
				});
		
		DataSet<Tuple3<String, List<Double>, Integer>> finalCentroidDS = iterDS.closeWith(updatedCentroids);
		
		finalCentroidDS = sampleDS
				.map(new MapSampleData()).withBroadcastSet(finalCentroidDS, "centroids")
				.groupBy(0)
				.sum(2)
				.join(finalCentroidDS)
				.where(0)
				.equalTo(0)
				.map(tuple -> {
					return new Tuple3<String, List<Double>, Integer>(tuple.f1.f0, tuple.f1.f1, tuple.f0.f2);
				});
		
		return finalCentroidDS;
	}
	
	public static class MapSampleData extends RichMapFunction<Tuple3<String, List<Double>, Integer>, Tuple3<String, List<Double>, Integer>> {
		private List<Tuple3<String, List<Double>, Integer>> centroids = new ArrayList<Tuple3<String, List<Double>, Integer>>();
		@Override
		public void open(Configuration parameters) throws Exception {
			this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
		}
		
		@Override
		public Tuple3<String, List<Double>, Integer> map(Tuple3<String, List<Double>, Integer> row) throws Exception {
			List<Double> features = row.f1;
			String pos = "0";
			double minDist = Double.MAX_VALUE;
			for (int i = 0; i < this.centroids.size(); i++) {
				List<Double> centroidFeatures = this.centroids.get(i).f1;
				double eucDist = 0;
				for (int j = 0; j < centroidFeatures.size(); j++) {
					eucDist += Math.pow(features.get(j) - centroidFeatures.get(j), 2);
				}
				eucDist = Math.sqrt(eucDist);
				
				if (eucDist < minDist) {
					pos = this.centroids.get(i).f0;
					minDist = eucDist;
				}
			}
			return new Tuple3<String, List<Double>, Integer>(pos, features, 1);
		}
	}

	public static final class ReduceSampleResearchers implements 
			GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>,
			GroupCombineFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

		@Override
		public void combine(Iterable<Tuple2<String, Integer>> tuples, Collector<Tuple2<String, Integer>> out)
				throws Exception {
			String name = "";
            int count = 0;
            for(Tuple2<String, Integer> tuple : tuples) {
                name = tuple.f0;
                count  += tuple.f1;
            }
            out.collect(new Tuple2<String, Integer>(name, count));
		}

		@Override
		public void reduce(Iterable<Tuple2<String, Integer>> tuples, Collector<Tuple2<String, Integer>> out)
				throws Exception {
			String name = "";
            int count = 0;
            for(Tuple2<String, Integer> tuple : tuples) {
                name = tuple.f0;
                count  += tuple.f1;
            }
            out.collect(new Tuple2<String, Integer>(name, count));
		}	
	}
	
}
