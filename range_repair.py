#!/usr/bin/env python
import operator
import optparse
import os
import re
import subprocess
import sys

def format_murmur(i):
    return "%020d" % i

def format_md5(i):
    return "%039d" % i

def lrange(num1, num2 = None, step = 1, format = format_murmur):
    offset = 0 if format == format_md5 else 2**63
    max = 2**127-1 if format == format_md5 else 2**63-1
    wrap = 2**128 if format == format_md5 else 2**64

    print "%d %d" % (num1+offset, num2+offset)
    while (num1 + offset < num2 + offset):
        yield num1
        num1 += step
	if num1 > max:
            num1 -= wrap

def run_command(command, *args):
    cmd = " ".join([command] + list(args))
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.wait()
    return proc.returncode == 0, proc.returncode, cmd, proc.stdout.read(), proc.stderr.read()

def is_murmur_ring(ring):
    for i in ring:
        if i < 0:
            return True

    return False

def get_ring_tokens():
    tokens = []
    success, return_code, _, stdout, stderr = run_command("nodetool", "ring")

    if not success:
        return False, [], stderr

    for line in stdout.split("\n")[6:]:
        segments = line.split()
        if len(segments) == 8:
            tokens.append(int(segments[-1]))

    return True, tokens, None

def get_host_token():
    success, return_code, _, stdout, stderr = run_command("nodetool", "info")
    if not success or stdout.find("Token") != 0:
        return False, None, stderr

    return True, int(stdout.split()[2]), None

def get_range_start(token, ring):
    return ring[(ring.index(token) -1 + len(ring)) % len(ring)]

def get_range_termination(token, ring):
    return token
#    for i in ring:
#        if token > i:
#            return i
#
#    if is_murmur_ring(ring):
#        return 2**63 - 1
#
#    return 2**127 - 1

def get_sub_range_generator(start, stop, steps=100, format=format_murmur):
    min = 0 if format == format_md5 else -2**63
    max = 2**127-1 if format == format_md5 else 2**63-1
    wrap = 2**128 if format == format_md5 else 2**64

    count = stop - start if stop > start else max-start + stop-min
    step_increment = count / steps

    done = 0
    for step in xrange(steps):
	if step == steps - 1:
            step_increment = count - done
	end = start + step_increment
	if end > max:
            end -= wrap
	    print "wrapping start=%d, increment=%d, max=%d, end=%d" % (start, step_increment, max, end)
	yield start, end
        done += step_increment
	start = end

#    for i in lrange(start + step_increment, stop + 1, step_increment, format):
#	print "start = %d, i = %d" % (start, i)
#        yield start, i
#        start = i

def repair_range(keyspace, start, end):
    success, return_code, cmd, stdout, stderr = \
        run_command("nodetool", "repair %s -local -snapshot -pr -st %s -et %s" % (keyspace, start, end))

    return success, cmd, stdout, stderr

def format_murmur(i):
    return "%020d" % i

def format_md5(i):
    return "%039d" % i

def repair_keyspace(keyspace, steps=100, verbose=True):
    success, ring_tokens, error = get_ring_tokens()
    if not success:
        print "Error fetching ring tokens"
        print error
        return False

    success, host_token, error = get_host_token()
    if not success:
        print "Error fetching host token"
        print error
        return False

    range_start = get_range_start(host_token, ring_tokens)
    range_termination = get_range_termination(host_token, ring_tokens)
    formatter = format_murmur if is_murmur_ring(ring_tokens) else format_md5

    if verbose:
        print "repair over range (%s, %s] with %s steps for keyspace %s" % (formatter(range_start), formatter(range_termination), steps, keyspace)

    for start, end in get_sub_range_generator(range_start, range_termination, steps):
        start = formatter(start)
        end = formatter(end)

        if verbose:
            print "step %04d repairing range (%s, %s] for keyspace %s ... " % (steps, start, end, keyspace),
        success, cmd, stdout, stderr = repair_range(keyspace, start, end)
        if not success:
            print "FAILED"
            print cmd
            print stderr
            return False
        if verbose:
            print "SUCCESS"
        steps -= 1

    return True

def main():
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-k", "--keyspace", dest="keyspace",
                      help="keyspace to repair", metavar="KEYSPACE")

    parser.add_option("-s", "--steps", dest="steps", type="int", default=100,
                      help="number of discrete ranges", metavar="STEPS")

    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose", default=True,
                      help="don't print status messages to stdout")

    (options, args) = parser.parse_args()

    if not options.keyspace:
        parser.print_help()
        sys.exit(1)

    if repair_keyspace(options.keyspace, options.steps, options.verbose):
        sys.exit(0)

    sys.exit(2)

if __name__ == '__main__':
    main()

# success, ring_tokens, error = get_ring_tokens()
# success, host_token, error = get_host_token()
# range_termination = get_range_termination(host_token, ring_tokens)
# steps = 100

# print repr(is_murmur_ring(ring_tokens))
# print repr(get_ring_tokens())
# print repr(get_host_token())
# print repr(get_range_termination(host_token, ring_tokens))
# print repr(get_sub_range_generator(host_token, range_termination, steps).next())
