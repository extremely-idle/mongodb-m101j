/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package course.homework;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.ascending;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

public class Homework3 {

    public static final String DB_NAME = "students";
    public static final String COLLECTION_NAME = "grades";
    public static final String STUDENT_ID_FIELD_NAME = "student_id";
    public static final String TYPE_FIELD_NAME = "type";
    public static final String SCORE_FIELD_NAME = "score";
    public static final String HOMEWORK_TYPE = "homework";

    public static void main(String[] args) {
        final MongoClient mongoClient = new MongoClient();

        final MongoDatabase mongoDatabase = mongoClient.getDatabase(DB_NAME);
        final MongoCollection<Document> gradesCollection = mongoDatabase.getCollection(COLLECTION_NAME);

        final List<Document> sortedList = gradesCollection.find(eq(TYPE_FIELD_NAME, HOMEWORK_TYPE))
                                                          .sort(ascending(STUDENT_ID_FIELD_NAME))
                                                          .sort(ascending(SCORE_FIELD_NAME))
                                                          .into(new ArrayList<>());

        final Map<Integer, List<Double>> sortedMap = sortedList.stream()
                                                               .collect(groupingBy(
                                                                       (Document d) -> d.getInteger(STUDENT_ID_FIELD_NAME),
                                                                       mapping((Document d) -> d.getDouble(SCORE_FIELD_NAME), toList())));

        for (Integer studentId : sortedMap.keySet()) {
            final List<Double> scoreList = sortedMap.get(studentId);
            final Optional<Double> lowestScoreOptional = ofNullable(scoreList.get(0));

            lowestScoreOptional.ifPresent((Double val) -> System.out.println("Lowest score for student " + studentId
                    + " is " + val + ", preparing to delete."));

            gradesCollection.deleteOne(and(
                eq(STUDENT_ID_FIELD_NAME, studentId),
                eq(TYPE_FIELD_NAME, HOMEWORK_TYPE),
                eq(SCORE_FIELD_NAME, lowestScoreOptional.get())));
        }
    }
}