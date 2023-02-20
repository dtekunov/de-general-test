## de-general-test

DISCLAIMER: Since I am not an AWS engineer, I could not test properly S3-oriented logic, and decided not to add it to the final code, but I can add some code it in a separate branch, because I had some experience in the past, working with downloading data in S3 (If it is really necessary)

### Prerequisites:

* Scala 2.13
* JVM 11

### Input arguments

*     --input <path-to-input-directory>
*     --output <path-to-output-directory>
*     --use-s3 # Boolean flag on whether data should be read/written from/to S3 or not (not implemented)

### Algorithms

1) V1 Algorithm uses storing values in a HashMap and hence has O(N) time and O(N) space complexity, but uses GroupByKey operation at a first step, which is really ineffective, because it requires data shuffling.
2) V1b Algorithm also has O(N) time complexity and requires data shuffling, but has O(1) space complexity for a second mapping step, because it does not use any in-between data structure.
3) V2 Algorithm is an adjusted distributed version of V1 algorithm, which still has O(N) time and O(N) space, but has no GroupByKey step and hence does not require data shuffling, which makes it more effective.
