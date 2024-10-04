# dataset-lambda-sns
This is a drop-in AWS lambda function. It is designed to be used as an SNS notification topic listener. 
It only uses standard libraries so you don't need to do any extra lambda config. Just copy and paste into a new lambda, add your ENV var, and you're done. 

The function calls the dataset.com / Scalyr API whenever an AWS SNS event occurs and posts the event into the logstream. 
This was built specifically to support SES email notification events but the solution is generalizable and should work for any AWS event data sent to it. 

