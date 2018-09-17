package org.madgik.MVTopicModel;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.lang.reflect.*;

import cc.mallet.classify.*;
import cc.mallet.classify.evaluate.*;
import cc.mallet.pipe.CharSequence2TokenSequence;
import cc.mallet.pipe.FeatureSequence2AugmentableFeatureVector;
import cc.mallet.pipe.Pipe;
import cc.mallet.pipe.SerialPipes;
import cc.mallet.pipe.Target2Label;
import cc.mallet.pipe.TokenSequence2FeatureSequence;
import cc.mallet.pipe.TokenSequenceLowercase;
import cc.mallet.pipe.TokenSequenceRemoveStopwords;
import cc.mallet.pipe.iterator.CsvIterator;
import cc.mallet.types.*;
import cc.mallet.util.*;
import java.nio.charset.Charset;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Classify spam documents, run trials & print statistics from a csv or tsv
 * file. The lines should have the following format: [instanceId label data]
 *
 * @author Omiros Metaxas </a>
 *
 *
 * --input ./SampleData/SMSSpamCollections2.txt --training-portion 0.9 --num-trials 10 --output-classifier ./SampleData/spam.classifier --trainer MaxEnt --trainer NaiveBayes
 * --input ./SampleData/SMSSpamCollections2.txt --cross-validation 10  --output-classifier ./SampleData/spam.classifier --trainer MaxEnt --trainer NaiveBayes
 * --trainer C45 --trainer DecisionTree --trainer MaxEntL1 --trainer NaiveBayesEM
 */
public abstract class SpamClassifier {

    static BshInterpreter interpreter = new BshInterpreter();

    private static Logger logger = MalletLogger.getLogger(SpamClassifier.class.getName());
    private static Logger progressLogger = MalletProgressMessageLogger.getLogger(SpamClassifier.class.getName() + "-pl");
    private static ArrayList<String> classifierTrainerStrings = new ArrayList<String>();

    public static String defaultLineRegex = "^(\\S*)[\\s,]*(\\S*)[\\s,]*(.*)$"; //Regular expression containing regex-groups for label, name and data whitespace, tab or comma delimeted 
    public static String defaultTokenRegex = "\\p{L}[\\p{L}\\p{P}]+\\p{L}";  //"Regular expression used for tokenization. Example: \"[\\p{L}\\p{N}_]+|[\\p{P}]+\" (unicode letters, numbers and underscore OR all punctuation) "
    //CharSequenceLexer.LEX_ALPHA.toString();  //

    static CommandOption.String trainerConstructor = new CommandOption.String(SpamClassifier.class, "trainer", "ClassifierTrainer constructor", true, "new NaiveBayesTrainer()",
            "Java code for the constructor used to create a ClassifierTrainer.  "
            + "If no '(' appears, then \"new \" will be prepended and \"Trainer()\" will be appended."
            + "You may use this option mutiple times to compare multiple classifiers.", null) {
        public void postParsing(CommandOption.List list) {
            classifierTrainerStrings.add(this.value);
        }
    };

    static CommandOption.String outputFile = new CommandOption.String(SpamClassifier.class, "output-classifier", "FILENAME", true, "classifier.mallet",
            "The filename in which to write the classifier after it has been trained.", null);

    /*	static CommandOption.String pipeFile = new CommandOption.String
     (Vectors2Classify.class, "output-pipe", "FILENAME", true, "classifier_pipe.mallet",
     "The filename in which to write the classifier's instancePipe after it has been trained.", null);*/
    static CommandOption.String inputFile = new CommandOption.String(SpamClassifier.class, "input", "FILENAME", true, "SMSSpamCollection.txt",
            "The tsv filename from which to read the list of training instances", null);

    static CommandOption.Double trainingProportionOption = new CommandOption.Double(SpamClassifier.class, "training-portion", "DECIMAL", true, 1.0,
            "The fraction of the instances that should be used for training.", null);

    static CommandOption.Double validationProportionOption = new CommandOption.Double(SpamClassifier.class, "validation-portion", "DECIMAL", true, 0.0,
            "The fraction of the instances that should be used for validation.", null);

    
    static CommandOption.Integer numTrialsOption = new CommandOption.Integer(SpamClassifier.class, "num-trials", "INTEGER", true, 1,
            "The number of random train/test splits to perform", null);

    static CommandOption.Integer verbosityOption = new CommandOption.Integer(SpamClassifier.class, "verbosity", "INTEGER", true, -1,
            "The level of messages to print: 0 is silent, 8 is most verbose. "
            + "Levels 0-8 correspond to the java.logger predefined levels "
            + "off, severe, warning, info, config, fine, finer, finest, all. "
            + "The default value is taken from the mallet logging.properties file,"
            + " which currently defaults to INFO level (3)", null);

    static CommandOption.Integer crossValidation = new CommandOption.Integer(SpamClassifier.class, "cross-validation", "INT", true, 0,
            "The number of folds for cross-validation (DEFAULT=0).", null);

    public static void main(String[] args) throws bsh.EvalError, java.io.IOException {
        // Process the command-line options
        CommandOption.setSummary(SpamClassifier.class,
                "A tool for training, saving and printing diagnostics from a classifier on vectors.");
        CommandOption.process(SpamClassifier.class, args);

        // handle default trainer here for now; default argument processing doesn't  work
        if (!trainerConstructor.wasInvoked()) {
            classifierTrainerStrings.add("new NaiveBayesTrainer()");
        }

        int verbosity = verbosityOption.value;

        Logger rootLogger = ((MalletLogger) progressLogger).getRootLogger();

        if (verbosityOption.wasInvoked()) {
            rootLogger.setLevel(MalletLogger.LoggingLevels[verbosity]);
        }

        //Load data & preprocess
        if (inputFile == null) {
            throw new IllegalArgumentException("You must include `--input FILE ...' in order to specify a"
                    + "file containing the instances, one per line.");
        }

        //Load already preprocessed training samples
        //InstanceList ilist = null;
        //ilist = InstanceList.load(new File(inputFile.value));
        //Define preprocessing steps...
        Pipe instancePipe;
        ArrayList<Pipe> pipeList = new ArrayList<Pipe>();
        pipeList.add(new Target2Label());

        // Tokenize the input: first compile the tokenization pattern
        // 
        Pattern tokenPattern = null;

        // Regular expression used for tokenization:   
        //Example: \"[\\p{L}\\p{N}_]+|[\\p{P}]+\" (unicode letters, numbers and underscore OR all punctuation) "
        try {
            tokenPattern = Pattern.compile(defaultTokenRegex);
        } catch (PatternSyntaxException pse) {
            throw new IllegalArgumentException("The token regular expression (" + defaultTokenRegex
                    + ") was invalid: " + pse.getMessage());
        }

        // Add the tokenizer
        pipeList.add(new CharSequence2TokenSequence(tokenPattern));

        // Normalize the input as necessary
        //convert to lower case
        pipeList.add(new TokenSequenceLowercase());

        // Use the default built-in English list
        TokenSequenceRemoveStopwords stopwordFilter
                = new TokenSequenceRemoveStopwords(false, false);
        pipeList.add(stopwordFilter);

        boolean keepSequence = false;
        if (keepSequence) {
            // Output is unigram feature sequences
            pipeList.add(new TokenSequence2FeatureSequence());
        } else {
            // Output is feature vectors (no sequence information)
            pipeList.add(new TokenSequence2FeatureSequence());
            pipeList.add(new FeatureSequence2AugmentableFeatureVector());
        }
        instancePipe = new SerialPipes(pipeList);

        //
        // Create the instance list and open the input file
        // 
        InstanceList ilist = new InstanceList(instancePipe);
        Reader fileReader;

        String encoding = Charset.defaultCharset().displayName(); //Character encoding for input file

        fileReader = new InputStreamReader(new FileInputStream(inputFile.value), encoding);
        ilist.addThruPipe(new CsvIterator(fileReader, Pattern.compile(defaultLineRegex), 3, 2, 1)); //data, label , name position
        // 

        /*
		// Save instances to a file 
		//

		ObjectOutputStream oos;		
                oos = new ObjectOutputStream(new FileOutputStream(inputFile.value+".mallet"));		
		oos.writeObject(instances);
		oos.close();
         */
        //--------------------------------------------------------------
        //Configure classification parameters & train classifiers 
        String labels[];

        labels = new String[ilist.getAlphabets()[1].size()];
        for (int k = 0; k < ilist.getAlphabets()[1].size(); k++) {
            labels[k] = (String) (ilist.getAlphabets()[1].toArray())[k];
            //System.out.println("Debug: labels.length "+ labels.length + " labels["+k+"] "+labels[k] );
        }

        if (crossValidation.wasInvoked() && trainingProportionOption.wasInvoked()) {
            logger.warning("Both --cross-validation and --training-portion were invoked.  Using cross validation with "
                    + crossValidation.value + " folds.");
        }
        if (crossValidation.wasInvoked() && validationProportionOption.wasInvoked()) {
            logger.warning("Both --cross-validation and --validation-portion were invoked.  Using cross validation with "
                    + crossValidation.value + " folds.");
        }
        if (crossValidation.wasInvoked() && numTrialsOption.wasInvoked()) {
            logger.warning("Both --cross-validation and --num-trials were invoked.  Using cross validation with "
                    + crossValidation.value + " folds.");
        }

        int numTrials;
        if (crossValidation.wasInvoked()) {
            numTrials = crossValidation.value;
        } else {
            numTrials = numTrialsOption.value;
        }

        Random r =  new Random();

        int numTrainers = classifierTrainerStrings.size();
        int numLabels = labels.length;

        double trainAccuracy[][] = new double[numTrainers][numTrials];
        double testAccuracy[][] = new double[numTrainers][numTrials];
        double validationAccuracy[][] = new double[numTrainers][numTrials];

        String trainConfusionMatrix[][] = new String[numTrainers][numTrials];
        String testConfusionMatrix[][] = new String[numTrainers][numTrials];
        String validationConfusionMatrix[][] = new String[numTrainers][numTrials];

        double trainPrecision[][][] = new double[numTrainers][numLabels][numTrials];
        double testPrecision[][][] = new double[numTrainers][numLabels][numTrials];
        double validationPrecision[][][] = new double[numTrainers][numLabels][numTrials];

        double trainRecall[][][] = new double[numTrainers][numLabels][numTrials];
        double testRecall[][][] = new double[numTrainers][numLabels][numTrials];
        double validationRecall[][][] = new double[numTrainers][numLabels][numTrials];

        double trainF1[][][] = new double[numTrainers][numLabels][numTrials];
        double testF1[][][] = new double[numTrainers][numLabels][numTrials];
        double validationF1[][][] = new double[numTrainers][numLabels][numTrials];

        double t = trainingProportionOption.value;
        double v = validationProportionOption.value;

        if (crossValidation.wasInvoked()) {
            logger.info("Cross-validation folds = " + crossValidation.value);
        } else {
            logger.info("Training portion = " + t);
            logger.info("Validation portion = " + v);
            logger.info("Testing portion = " + (1 - v - t));
        }

        CrossValidationIterator cvIter;
        if (crossValidation.wasInvoked()) {
            if (crossValidation.value < 2) {
                throw new RuntimeException("At least two folds (set with --cross-validation) are required for cross validation");
            }
            cvIter = new CrossValidationIterator(ilist, crossValidation.value, r);
        } else {
            cvIter = null;
        }

        String[] trainerNames = new String[numTrainers];
        for (int trialIndex = 0; trialIndex < numTrials; trialIndex++) {
            System.out.println("\n-------------------- Trial " + trialIndex + "  --------------------\n");
            InstanceList[] ilists;

            if (crossValidation.wasInvoked()) {
                InstanceList[] cvSplit = cvIter.next();
                ilists = new InstanceList[3];
                ilists[0] = cvSplit[0];
                ilists[1] = cvSplit[1];
                ilists[2] = cvSplit[0].cloneEmpty();
            } else {
                ilists = ilist.split(r, new double[]{t, 1 - t - v, v});
            }

            long time[] = new long[numTrainers];
            for (int c = 0; c < numTrainers; c++) {
                time[c] = System.currentTimeMillis();
                ClassifierTrainer trainer = getTrainer(classifierTrainerStrings.get(c));
                trainer.setValidationInstances(ilists[2]);
                System.out.println("Trial " + trialIndex + " Training " + trainer + " with " + ilists[0].size() + " instances");

                Classifier classifier = trainer.train(ilists[0]);

                System.out.println("Trial " + trialIndex + " Training " + trainer.toString() + " finished");
                time[c] = System.currentTimeMillis() - time[c];
                Trial trainTrial = new Trial(classifier, ilists[0]);
                //assert (ilists[1].size() > 0);
                Trial testTrial = new Trial(classifier, ilists[1]);
                Trial validationTrial = new Trial(classifier, ilists[2]);

                if (ilists[0].size() > 0) {
                    trainConfusionMatrix[c][trialIndex] = new ConfusionMatrix(trainTrial).toString();
                    trainAccuracy[c][trialIndex] = trainTrial.getAccuracy();
                    for (int k = 0; k < labels.length; k++) {
                        trainPrecision[c][k][trialIndex] = trainTrial.getPrecision(labels[k]);
                        trainRecall[c][k][trialIndex] = trainTrial.getRecall(labels[k]);
                        trainF1[c][k][trialIndex] = trainTrial.getF1(labels[k]);
                    }
                }
                if (ilists[1].size() > 0) {
                    testConfusionMatrix[c][trialIndex] = new ConfusionMatrix(testTrial).toString();
                    testAccuracy[c][trialIndex] = testTrial.getAccuracy();

                    for (int k = 0; k < labels.length; k++) {
                        testPrecision[c][k][trialIndex] = testTrial.getPrecision(labels[k]);
                        testRecall[c][k][trialIndex] = testTrial.getRecall(labels[k]);
                        testF1[c][k][trialIndex] = testTrial.getF1(labels[k]);
                    }
                }
                if (ilists[2].size() > 0 && v > 0) {
                    validationConfusionMatrix[c][trialIndex] = new ConfusionMatrix(validationTrial).toString();
                    validationAccuracy[c][trialIndex] = validationTrial.getAccuracy();
                    for (int k = 0; k < labels.length; k++) {
                        validationPrecision[c][k][trialIndex] = validationTrial.getPrecision(labels[k]);
                        validationRecall[c][k][trialIndex] = validationTrial.getRecall(labels[k]);
                        validationF1[c][k][trialIndex] = validationTrial.getF1(labels[k]);
                    }
                }

             
                if (outputFile.wasInvoked()) {
                    String filename = outputFile.value;
                    if (numTrainers > 1) {
                        filename = filename + trainer.toString();
                    }
                    if (numTrials > 1) {
                        filename = filename + ".trial" + trialIndex;
                    }
                    try {
                        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filename));
                        oos.writeObject(classifier);
                        oos.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new IllegalArgumentException("Couldn't write classifier to filename "
                                + filename);
                    }
                }

                String label = "spam";

                //train
                if (ilists[0].size() > 0) {
                    System.out.println("Trial " + trialIndex + " Trainer " + trainer.toString() + " Training Data Confusion Matrix");
                    System.out.println(trainConfusionMatrix[c][trialIndex]);

                    System.out.println("Trial " + trialIndex + " Trainer " + trainer.toString() + " training data accuracy= " + trainAccuracy[c][trialIndex]);

                    for (int k = 0; k < labels.length; k++) {
                        System.out.println("Trial " + trialIndex + " Trainer " + trainer.toString() + " training data Precision(" + labels[k] + ") = " + trainTrial.getPrecision(labels[k]));
                        System.out.println("Trial " + trialIndex + " Trainer " + trainer.toString() + " training data Recall(" + labels[k] + ") = " + trainTrial.getRecall(labels[k]));
                        System.out.println("Trial " + trialIndex + " Trainer " + trainer.toString() + " training data F1(" + labels[k] + ") = " + trainTrial.getF1(labels[k]));
                    }

                }

                //test
                if (ilists[1].size() > 0) {
                    System.out.println("Trial " + trialIndex + " Trainer " + trainer.toString() + " Test Data Confusion Matrix");
                    System.out.println(testConfusionMatrix[c][trialIndex]);

                    System.out.println("Trial " + trialIndex + " Trainer " + trainer.toString() + " test data accuracy= " + testAccuracy[c][trialIndex]);

                    for (int k = 0; k < labels.length; k++) {
                        System.out.println("Trial " + trialIndex + " Trainer " + trainer.toString() + " test data precision(" + labels[k] + ") = " + testTrial.getPrecision(labels[k]));
                        System.out.println("Trial " + trialIndex + " Trainer " + trainer.toString() + " test data recall(" + labels[k] + ") = " + testTrial.getRecall(labels[k]));
                        System.out.println("Trial " + trialIndex + " Trainer " + trainer.toString() + " test data F1(" + labels[k] + ") = " + testTrial.getF1(labels[k]));
                    }
                }

                //validation
                if (ilists[2].size() > 0 && v > 0) {

                    System.out.println("Trial " + trialIndex + " Trainer " + trainer.toString() + " Validation Data Confusion Matrix");
                    System.out.println(validationConfusionMatrix[c][trialIndex]);

                    System.out.println("Trial " + trialIndex + " Trainer " + trainer.toString() + " validation data accuracy= " + validationAccuracy[c][trialIndex]);

                    for (int k = 0; k < labels.length; k++) {
                        System.out.println("Trial " + trialIndex + " Trainer " + trainer.toString() + " validation data precision(" + labels[k] + ") = " + validationTrial.getPrecision(labels[k]));
                        System.out.println("Trial " + trialIndex + " Trainer " + trainer.toString() + " validation data recall(" + labels[k] + ") = " + validationTrial.getRecall(labels[k]));
                        System.out.println("Trial " + trialIndex + " Trainer " + trainer.toString() + " validation data F1(" + labels[k] + ") = " + validationTrial.getF1(labels[k]));
                    }

                }

                if (trialIndex == 0) {
                    trainerNames[c] = trainer.toString();
                }

            }  // end for each trainer
        }  // end for each trial

        // New reporting
        for (int c = 0; c < numTrainers; c++) {
            System.out.println("\n" + trainerNames[c].toString());

            System.out.println("Summary. train accuracy mean = " + MatrixOps.mean(trainAccuracy[c])
                    + " stddev = " + MatrixOps.stddev(trainAccuracy[c])
                    + " stderr = " + MatrixOps.stderr(trainAccuracy[c]));

            for (int k = 0; k < labels.length; k++) {
                System.out.println("Summary. train precision(" + labels[k] + ") mean = " + MatrixOps.mean(trainPrecision[c][k])
                        + " stddev = " + MatrixOps.stddev(trainPrecision[c][k])
                        + " stderr = " + MatrixOps.stderr(trainPrecision[c][k]));

                System.out.println("Summary. train recall(" + labels[k] + ") mean = " + MatrixOps.mean(trainRecall[c][k])
                        + " stddev = " + MatrixOps.stddev(trainRecall[c][k])
                        + " stderr = " + MatrixOps.stderr(trainRecall[c][k]));
                System.out.println("Summary. train f1(" + labels[k] + ") mean = " + MatrixOps.mean(trainF1[c][k])
                        + " stddev = " + MatrixOps.stddev(trainF1[c][k])
                        + " stderr = " + MatrixOps.stderr(trainF1[c][k]));

            }

            if (v > 0) {
                System.out.println("Summary. validation accuracy mean = " + MatrixOps.mean(validationAccuracy[c])
                        + " stddev = " + MatrixOps.stddev(validationAccuracy[c])
                        + " stderr = " + MatrixOps.stderr(validationAccuracy[c]));

                for (int k = 0; k < labels.length; k++) {
                    System.out.println("Summary. validation precision(" + labels[k] + ") mean = " + MatrixOps.mean(validationPrecision[c][k])
                            + " stddev = " + MatrixOps.stddev(validationPrecision[c][k])
                            + " stderr = " + MatrixOps.stderr(validationPrecision[c][k]));

                    System.out.println("Summary. validation recall(" + labels[k] + ") mean = " + MatrixOps.mean(validationRecall[c][k])
                            + " stddev = " + MatrixOps.stddev(validationRecall[c][k])
                            + " stderr = " + MatrixOps.stderr(validationRecall[c][k]));

                    System.out.println("Summary. validation f1(" + labels[k] + ") mean = " + MatrixOps.mean(validationF1[c][k])
                            + " stddev = " + MatrixOps.stddev(validationF1[c][k])
                            + " stderr = " + MatrixOps.stderr(validationF1[c][k]));

                }
            }

            System.out.println("Summary. test accuracy mean = " + MatrixOps.mean(testAccuracy[c])
                    + " stddev = " + MatrixOps.stddev(testAccuracy[c])
                    + " stderr = " + MatrixOps.stderr(testAccuracy[c]));

            for (int k = 0; k < labels.length; k++) {
                System.out.println("Summary. test precision(" + labels[k] + ") mean = " + MatrixOps.mean(testPrecision[c][k])
                        + " stddev = " + MatrixOps.stddev(testPrecision[c][k])
                        + " stderr = " + MatrixOps.stderr(testPrecision[c][k]));

                System.out.println("Summary. test recall(" + labels[k] + ") mean = " + MatrixOps.mean(testRecall[c][k])
                        + " stddev = " + MatrixOps.stddev(testRecall[c][k])
                        + " stderr = " + MatrixOps.stderr(testRecall[c][k]));

                System.out.println("Summary. test f1(" + labels[k] + ") mean = " + MatrixOps.mean(testF1[c][k])
                        + " stddev = " + MatrixOps.stddev(testF1[c][k])
                        + " stderr = " + MatrixOps.stderr(testF1[c][k]));
            }
        }   // end for each trainer
    }

    private static void printTrialClassification(Trial trial) {
        for (Classification c : trial) {
            Instance instance = c.getInstance();
            System.out.print(instance.getName() + " " + instance.getTarget() + " ");
            Labeling labeling = c.getLabeling();
            for (int j = 0; j < labeling.numLocations(); j++) {
                System.out.print(labeling.getLabelAtRank(j).toString() + ":" + labeling.getValueAtRank(j) + " ");
            }
            System.out.println();
        }
    }

    private static Object createTrainer(String arg) {
        try {
            return interpreter.eval(arg);
        } catch (bsh.EvalError e) {
            throw new IllegalArgumentException("Java interpreter eval error\n" + e);
        }
    }

    private static ClassifierTrainer getTrainer(String arg) {
        // parse something like Maxent,gaussianPriorVariance=10,numIterations=20

        // first, split the argument at commas.
        java.lang.String fields[] = arg.split(",");

        //Massage constructor name, so that MaxEnt, MaxEntTrainer, new MaxEntTrainer()
        // all call new MaxEntTrainer()
        java.lang.String constructorName = fields[0];
        Object trainer;
        if (constructorName.indexOf('(') != -1) // if contains (), pass it though
        {
            trainer = createTrainer(arg);
        } else if (constructorName.endsWith("Trainer")) {
            trainer = createTrainer("new " + constructorName + "()"); // add parens if they forgot
        } else {
            trainer = createTrainer("new " + constructorName + "Trainer()"); // make trainer name from classifier name
        }

        // find methods associated with the class we just built
        Method methods[] = trainer.getClass().getMethods();

        // find setters corresponding to parameter names.
        for (int i = 1; i < fields.length; i++) {
            java.lang.String nameValuePair[] = fields[i].split("=");
            java.lang.String parameterName = nameValuePair[0];
            java.lang.String parameterValue = nameValuePair[1];  //todo: check for val present!
            java.lang.Object parameterValueObject;
            try {
                parameterValueObject = interpreter.eval(parameterValue);
            } catch (bsh.EvalError e) {
                throw new IllegalArgumentException("Java interpreter eval error on parameter "
                        + parameterName + "\n" + e);
            }

            boolean foundSetter = false;
            for (int j = 0; j < methods.length; j++) {
                // System.out.println("method " + j + " name is " + methods[j].getName());
                // System.out.println("set" + Character.toUpperCase(parameterName.charAt(0)) + parameterName.substring(1));
                if (("set" + Character.toUpperCase(parameterName.charAt(0)) + parameterName.substring(1)).equals(methods[j].getName())
                        && methods[j].getParameterTypes().length == 1) {
                    // System.out.println("Matched method " + methods[j].getName());
                    // Class[] ptypes = methods[j].getParameterTypes();
                    // System.out.println("Parameter types:");
                    // for (int k=0; k<ptypes.length; k++){
                    // System.out.println("class " + k + " = " + ptypes[k].getName());
                    // }

                    try {
                        java.lang.Object[] parameterList = new java.lang.Object[]{parameterValueObject};
                        // System.out.println("Argument types:");
                        // for (int k=0; k<parameterList.length; k++){
                        // System.out.println("class " + k + " = " + parameterList[k].getClass().getName());
                        // }
                        methods[j].invoke(trainer, parameterList);
                    } catch (IllegalAccessException e) {
                        System.out.println("IllegalAccessException " + e);
                        throw new IllegalArgumentException("Java access error calling setter\n" + e);
                    } catch (InvocationTargetException e) {
                        System.out.println("IllegalTargetException " + e);
                        throw new IllegalArgumentException("Java target error calling setter\n" + e);
                    }
                    foundSetter = true;
                    break;
                }
            }
            if (!foundSetter) {
                System.out.println("Parameter " + parameterName + " not found on trainer " + constructorName);
                System.out.println("Available parameters for " + constructorName);
                for (int j = 0; j < methods.length; j++) {
                    if (methods[j].getName().startsWith("set") && methods[j].getParameterTypes().length == 1) {
                        System.out.println(Character.toLowerCase(methods[j].getName().charAt(3))
                                + methods[j].getName().substring(4));
                    }
                }

                throw new IllegalArgumentException("no setter found for parameter " + parameterName);
            }
        }
        assert (trainer instanceof ClassifierTrainer);
        return ((ClassifierTrainer) trainer);
    }
}
