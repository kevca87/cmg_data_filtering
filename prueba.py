verbs = {
        'add':0,
        'fix':0,
        'use':0,
        'update':0,
        'remove':0,
        'make':0,
        'change':0,
        'move':0,
        'allow':0,
        'improve':0,
        'implement':0,
        'create':0,
        'upgrade':0
    }

messageverb = 'juanito'
print(len(verbs))

try:
    verbs[messageverb] += 1
except:
    messageverb = None
print(len(verbs))
print(verbs)