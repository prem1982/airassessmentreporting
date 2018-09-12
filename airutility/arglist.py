'''
Created on May 1, 2013

@author: temp_dmenes
'''

import cStringIO
import tokenize

__all__ = [ 'parse_arg_list' ]


def parse_arg_list( src ):
    '''Parses a string as if it were the argument list of a function

    Returns a tuple containing a list and a dict, ( arg, argv ), suitable
    for calling a function as f( *arg, **argv )
    '''
    io = cStringIO.StringIO( src )
    arg = []
    argv = {}
    expect_end = True
    expect_value = True
    expect_name = True
    expect_equal = False
    expect_comma = False
    positional_ok = True
    pending_name = None
    for ( tok_type, tok_string, x, y, z ) in tokenize.generate_tokens( lambda : io.readline() ) :
        if tok_type in ( tokenize.STRING, tokenize.NUMBER ):
            if not expect_value:
                raise ValueError( "Unexpected value {} found at column {} line {} in argument list {}".format( tok_string, x[1], x[0], z ) )
            if pending_name is None:
                arg.append( eval( tok_string ) )
            else:
                argv[ pending_name ] = eval( tok_string )
            expect_end = True
            expect_value = False
            expect_name = False
            expect_equal = False
            expect_comma = True
            
        elif tok_type == tokenize.OP:
            if tok_string == ',' and expect_comma:
                expect_end = False
                expect_value = positional_ok
                expect_name = True
                expect_equal = False
                expect_comma = False
            elif tok_string == '=' and expect_equal:
                expect_end = False
                expect_value = True
                expect_name = False
                expect_equal = False
                expect_comma = False
            else:
                raise ValueError( "Unexpected operator {} found at column {} line {} in argument list {}".format( tok_string, x[1], x[0], z ) )
        elif tok_type == tokenize.NAME:
            if not expect_name:
                raise ValueError( "Unexpected identifier {} found at column {} line {} in argument list {}".format( tok_string, x[1], x[0], z ) )
            if tok_string in argv:
                raise ValueError( "Duplicate keyword arg {} found at column {} line {} in argument list {}".format( tok_string, x[1], x[0], z ) )
            pending_name = tok_string
            positional_ok = False
            expect_end = False
            expect_value = False
            expect_name = False
            expect_equal = True
            expect_comma = False
            
        elif tok_type == tokenize.ENDMARKER:
            if not expect_end:
                raise ValueError( "Unexpected end of argument list {}".format( z ) )
            # We will now exit the loop
            
        elif tok_type in ( tokenize.NEWLINE, tokenize.INDENT ):
            pass
            # We ignore whitespace 
        
    return arg, argv