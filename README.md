#Broom.py: Sweep parameters easily

A simple declarative API for enumerating (possibly constrained) combinations of values, implemented in python.

## API

### def sweep(*things) 
Takes a list of arguments, each of which must be either
1) a constant
2) a key-value pair 
    where the key can be made a string by str()
    and the value is either 
    1) a constant value
    2) a list (or similar) over items that can be made strings by str()
    3) a function that returns a generator over values given 'state', a 
        map containing values for all key-value pairs that appear previous 
        to this pair in the list of arguments. This allows values to 
        depend on (be constrained by) other parts of the state, making 
        this iterator slightly smarter than a naive cartesian product 
        over sets.
3) None: these values are ignored

Yields all possible pairs of keys,state where 'keys' encodes the original ordering 
of arguments, and 'state' is a map containing a value for each key. 
int value keys are generated automaticaly for constant arguments.

Each keys,state pair may be converted into a string by join(keys,state)



### def join(keys,state,delim=' ',equals='=')
Assemble keys,state pairs such as result from sweep() into a string 
such as "key1=val1 key2=val2 key3=val3 val4".

Notes:
    Keys define the order of the items in the string.
    State determines the values. Integer keys are 
        assumed to have been generated randomly by sweep() and 
        are removed from the string like val4 in the example above.



### class Mapper() 
A convenience generator that uses a dict to impose a 
deterministic mapping from values in the state. 
For example, I might know that whenever state['dataset']=='big'
I want state['size'] to be 1000, and when state['dataset']=='small' 
I want the state['size'] to be 10. Then I could pass in the 
following to sweep():
sweep(
    ('dataset',('big','small)),
    ('size',Mapper('dataset',{'big':1000,'small':10}).generator),
)

Note:
    If the value is a tuple rather than a constant, all values are yielded.
    If you want to match substrings, set matchsubstrings=False. 
        When multiple substrings match, only the first is used.
    If you only want to specify exceptions, set default=value



### class Range() 

Generates a list of numbers. Usage: 
sweep(
    ('--something',Map(1,6).generator)
)



### def shorten_option(option,maxlength=5) 
Expects a standard option string ('--some-long-option'). 
Returns a shortened version by taking the first letter of each word 
and then as many characters of the last word as possible ('slopt').

This function is mostly separate from the rest of the API, but 
may come in handy when you are attempting to summarize long lists 
of parameters.



## Example Usage 
```
class CustomValues():
    def __init__(self,somearg):
        self.somearg=somearg
    def generator(self,keys,state):
        for i in range(2):
            yield "%s%s" % (i,self.somearg)

def outgen(keys,state):
    yield '> %s-%s-%s.csv' % (state['--a'],state['--b'],state['--c'])

if __name__=="__main__":
    sweeper = sweep(
        'command',
        ('--constant-option','pink'),
        ('--a',('zero','one')),
        ('--b',CustomValues('custom').generator),
        ('--c',Mapper('--a',{'zero':'ais0','one':'ais1'}).generator),
        outgen,
    )

    for keys,state in sweeper:
        print join(keys,state)

    # demonstrate shorten_option (totally separate from the rest)
    print shorten_option('--some-long-option') # --> slopt

```


Output
```
command --constant-option=pink --a=zero --b=0custom --c=ais0 > zero-0custom-ais0.csv
command --constant-option=pink --a=zero --b=1custom --c=ais0 > zero-1custom-ais0.csv
command --constant-option=pink --a=one --b=0custom --c=ais1 > one-0custom-ais1.csv
command --constant-option=pink --a=one --b=1custom --c=ais1 > one-1custom-ais1.csv
slopt
```

