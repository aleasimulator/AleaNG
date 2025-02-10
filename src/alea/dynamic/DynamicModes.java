/*
 Copyright (c) 2015 Simon Toth (kontakt@simontoth.cz)

 Permission is hereby granted, free of charge, to any person obtaining a copy of
 this software and associated documentation files (the "Software"), to deal in
 the Software without restriction, including without limitation the rights to
 use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 of the Software, and to permit persons to whom the Software is furnished to do
 so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
 */
package alea.dynamic;

/**
 * Styles of handling dynamic batches.
 *
 * @author Simon Toth (kontakt@simontoth.cz)
 * @author Dalibor Klusacek (klusacek@cesnet.cz) - code fixes and updates + documentation
 */
public enum DynamicModes {

    /**
     * Process batches as soon as dependencies are unblocked. 
     * No think times, just release next batch. Sessions are not used at all.
     * 
     */
    DEPENDENCY_ASAP,
    
    /**
     * Processes batches in a dynamic way but respects existing sessions as they were detected in the workload.
     * Currently, only this option uses sessions.
     */
    DEPENDENCY_NATURAL_STATIC,
    
    /** Processes batches flexibly but respects static sessions.
     * Unused now.
     */
    DEPENDENCY_NATURAL_FLEXIBLE,
    
    /**
     * Processes batches as dependencies are unblocked, but follow sessions. 
     * Unused now.
     */
    DEPENDENCY_SESSION
}
