#!/usr/bin/python
# Copyright 2015 Paul Felt
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import re

#################################
# Support functions
#################################
def cartesian_product(keys,state,entries):
    # base case
    if len(entries)==0:
        yield state
    # recurse
    else:
        # copy state (so that we don't get weird mutation things happening)
        state = state.copy()
        keys_in_state = [k for k in keys if k in state.keys()]
        # each entry has a state key and a function that produces a value generator
        key,f = entries[0]
        value_generator = f(keys_in_state,state)
        # iterate over each value for the outermost key, recurse on the remainder
        for val in value_generator:
            state[key] = val
            for inner_state in cartesian_product(keys,state,entries[1:]):
                yield inner_state

# static generators that don't depend on state
class ConstantGenerator:
    def __init__(self,val):
        self.val = val
    def genmaker(self,keys,state):
        yield self.val

class ListGenerator:
    def __init__(self,lst):
        self.lst = lst
    def genmaker(self,keys,state):
        for item in self.lst:
            yield item

# keys for constant things that only have a value
def keygenerator():
    i=0
    while True:
        i+=1
        yield i

#################################
# Main API function
#################################
def sweep(*things):
    ''' Takes a list of arguments, each of which must be either
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
    '''

    # convert everything into key-value pairs
    # singleton things get assigned a unique numerical key
    keygen = keygenerator()
    keys = [] # track key order 
    pairs = []
    for thing in things:
        if thing is not None: # ignore Nones
            if isinstance(thing,(list,tuple)):
                keys.append(str(thing[0])) # key is str
                pairs.append((thing[0],thing[1]))
                if len(thing)!=2:
                    raise Exception("Everything passed to sweep() must be constants or else key-value pairs (length==2)\n\tBad argument:%s"%str(thing))
            else:
                key = keygen.next() # key is int
                keys.append(key)
                pairs.append((key,thing))

    # convert each pair to be key,function where 
    # function creates a value generator based on the state 
    # of keys occuring previously in the list
    entries = []
    for key,val in pairs:
        # already in the right form
        if callable(val):
            entries.append((key,val))
        # list
        elif isinstance(val,(list,tuple)):
            entries.append((key,ListGenerator(val).genmaker))
        # other (assume value is a constant with value str())
        else:
            entries.append((key,ConstantGenerator(val).genmaker))

    # cartesian product over states
    for state in cartesian_product(keys,{},entries):
        yield keys,state

def join(keys,state,delim=' ',equals='='):
    ''' 
    Assemble keys,state pairs such as result from sweep() into a string 
    such as "key1=val1 key2=val2 key3=val3 val4". 

    Notes:
        Keys define the order of the items in the string.
        State determines the values. Integer keys are 
            assumed to have been generated automatically by sweep() and 
            are removed from the string like val4 in the example above. 
        If state[key]==None, the key value is not printed at all.
        If state[key]=='', the key value is printed but the equals and value are omitted.
    '''
    # key,value pair -> option string
    optlist = []
    for key in keys:
        if state[key] is not None: # ignore keys whose value is None
            val = str(state[key]) # val -> str
            # 'val' (auto-key)
            if isinstance(key,int):
                optlist.append(val)
            # 'key' (empty string values)
            elif len(val)==0:
                optlist.append(key)
            # 'key=value' (normal key-val pair)
            else:
                optlist.append(equals.join((key,val)))

    # option strings -> command string
    return delim.join(optlist)

class Mapper():
    '''
    A convenience generator that uses a dict to impose a 
    deterministic mapping from values in the state. 
    For example, I might know that whenever state['dataset']=='big'
    I want state['size'] to be 1000, and when state['dataset']=='small' 
    I want the state['size'] to be 10. Then I could pass in the 
    following to sweep():
    sweep(
        ('dataset',('big','small)),
        ('size',Mapper('dataset',{'big':1000,'s.*l':10}).generator),
    )

    Notes:
        - Regular expressions are allowed (see above example).
        - If the value is a tuple rather than a constant, all values are yielded.
        - If multiple patterns match, an error is raised.
        - The default value (if specified) is returned when nothing is matched.
    '''
    def __init__(self,statekey,valmapping,default=None):
        self.statekey=statekey
        self.valmapping=valmapping
        self.default=default
    def list_yield(self,thing):
        if isinstance(thing,(list,tuple)):
            for item in thing:
                yield item
        else:
            yield thing
    def generator(self,keys,state):
        stateval = state[self.statekey]
        # yield all mappings that contain substrings of the state value
        didyield = False
        for mapkey,mapval in self.valmapping.items():
            assert isinstance(mapkey,str), "the mapping key %s is not a string but rather a %s"%(mapkey,str(type(mapkey)))
            if stateval is not None and re.match(mapkey,stateval):
                assert not didyield, "multiple keys matched the value %s for Mapper(%s) matched multiple values"%(stateval,self.statekey)
                for yld in self.list_yield(mapval):
                    yield yld
                didyield = True

        # otherwise, default
        if not didyield:
            for yld in self.list_yield(self.default):
                yield yld

class Range():
    ''''
    Generates a list of numbers. Usage: 
    sweep(
        ('--something',Range(1,6).generator)
    )
    '''
    def __init__(self,start,end):
        self.start=start
        self.end=end
    def generator(self,keys,state):
        for r in range(self.start,self.end):
            yield r

def shorten_option(option,maxlength=5):
    '''
    Expects a standard option string ('--some-long-option'). 
    Returns a shortened version by taking the first letter of each word 
    and then as many characters of the last word as possible ('slopt').
    '''
    short = []
    cumlength = 0

    words = option.split('-')
    for i,part in enumerate(words):
        # last word keep as much as possible
        # intermediate word keep a single letter
        numchars = maxlength if i==len(words)-1 else 1
        # make sure length is legal
        numchars = min(numchars,maxlength-cumlength,len(part))
        # add selected bit
        bit = part[:numchars]
        short.append(bit)
        cumlength += len(bit)
    return ''.join(short)

#################################
# Demo usage
#################################
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
        ('--nota',Mapper('--a',{'zero':'','one':None}).generator),
        outgen,
    )

    for keys,state in sweeper:
        print join(keys,state)

    # demonstrate shorten_option (totally separate from the rest)
    print shorten_option('--some-long-option') # --> slopt

