"""
Module for commonly-used functions
"""

def user_choice(choice_list):
    """
    """
    choices = dict(enumerate(choice_list))
    display = '\n'.join(["[%d] %s"%(k,v) for
                        k,v in choices.items()])
    print(display)
    n = input("Choose one of %s"%', '.join([str(k) 
              for k in choices.keys()]))
    choice = choices[int(n)]
    print("\nSelected: %s"%choice)
    return choice