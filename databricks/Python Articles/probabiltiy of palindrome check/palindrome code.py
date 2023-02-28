def check_probability_of_palindrome(s):
    dictonary1=dict()

    for i in list(s):
        if i in dictonary1:
            dictonary1[i]+=1
        else:
            dictonary1[i]=1
    
    k=[]
    for i,j in dictonary1.items():
        k.append(j)
        
    k1=[]
    for i in k:
        if i%2!=0:
            k1.append(i)
    if len(k1)==1:
        print(True)
    else:
        print(False)
		
		
https://gist.github.com/sairamdgr8/3f98b4b20417ff1d770e4f81473f6c1a
    
https://www.linkedin.com/pulse/python-interview-questions-series-1-sairam-p-l


	
	Python interview questions Series-1 checking the probability of a given string whether it can form a palindrome or not
	
	In this article, we are going to discuss the palindrome probability for a given string.

A palindrome is a word, number, phrase, or another sequence of characters which reads the same backward as forward, such as madam or racecar.

Let's get into the scenario question coding :-



def check_probability_of_palindrome(s)
	    dictonary1=dict()
	

	    for i in list(s):
	        if i in dictonary1:
	            dictonary1[i]+=1
	        else:
	            dictonary1[i]=1
	    
	    k=[]
	    for i,j in dictonary1.items():
	        k.append(j)
	        
	    k1=[]
	    for i in k:
	        if i%2!=0:
	            k1.append(i)
	    if len(k1)==1:
	        print(True)
	    else:
	        print(False)
	

	# check        
	check_probability_of_palindrome("abb")
	>>> True
	

	## other examples
	# Input : "mdaam"  
	# output: true
	# Input : "abb"
	# Output : true
	# Input : "civic"
	# output : true
	# input : "rock"
	# output : false:
That's all for now...

This Code might look navie but serves the purpose.

Note:- If anyone has a better approach to generalizing this code happy to embed it in my script.

That’s all for now…Happy Learning….

Please do like and follow back my profile…Don't forget to Comment…

Working-with-probabilty-of-palindrome-check-in-given-string-using-python
