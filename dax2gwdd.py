#MIT License
#
#Copyright (c) 2017 Juan J. Durillo
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.

import sys
import os
import xml.etree.ElementTree as ET
import collections
from xml.dom import minidom
import ntpath


def buildFilesDictionary(files):
    files_dictionary = {}
    # a file has a single element pfn with the url of the file
    for file in files:
        files_dictionary[file.attrib['name']] = file[0].attrib['url']

    return files_dictionary
####buildFilesDictionary

def buildExecutablesDictionary(executables):
    executables_dictionary = {}
    # a executable has a single element pfn with the url of the file
    for executable in executables:
        executables_dictionary[executable.attrib['name']] = executable[0].attrib['url']

    return executables_dictionary
####buildExecutablesDictionary


def buildJobsDictionary(jobs,dependencies):
    jobs_dictionary = collections.OrderedDict()
    for job in jobs:
        jobs_dictionary[job.attrib['id']] = {}
        # obtaining executable
        jobs_dictionary[job.attrib['id']]['executable'] = job.attrib['name']
        # obtaining arguments
        jobs_dictionary[job.attrib['id']]['arguments'] = []
        argument = job.find('{http://pegasus.isi.edu/schema/DAX}argument')
        #print(job.attrib['id'])
        if argument is not None:
            sub_files = argument.findall('{http://pegasus.isi.edu/schema/DAX}file')
            if len(sub_files) == 0:
                # this is a hardcoded argument
                jobs_dictionary[job.attrib['id']]['arguments'].append(argument.text)
            else:
                i = 0
                for text in argument.itertext():
                    #print(text)
                    if i < len(sub_files):
                        jobs_dictionary[job.attrib['id']]['arguments'].append(([text,sub_files[i].attrib['name']]))
                        i = i + 1
                    else:
                        jobs_dictionary[job.attrib['id']]['arguments'].append((text))
                    #print(jobs_dictionary[job.attrib['id']]['arguments'])




        # processing inputs and outputs
        jobs_dictionary[job.attrib['id']]['inputs'] = []
        jobs_dictionary[job.attrib['id']]['outputs'] = []


        uses = job.findall('{http://pegasus.isi.edu/schema/DAX}uses')
        for use in uses:
            if use.attrib['link'] == "input":
                 jobs_dictionary[job.attrib['id']]['inputs'].append(use.attrib['name'])
            else:
                jobs_dictionary[job.attrib['id']]['outputs'].append(use.attrib['name'])

        jobs_dictionary[job.attrib['id']]['depends'] = []
        jobs_dictionary[job.attrib['id']]['unmetDependencies'] = []
        jobs_dictionary[job.attrib['id']]['parent'] = []
        jobs_dictionary[job.attrib['id']]['executed'] = False

    #adding dependencies
    for dependence in dependencies:
        parents = dependence.findall('{http://pegasus.isi.edu/schema/DAX}parent')
        for parent in parents:
            jobs_dictionary[dependence.attrib['ref']]['depends'].append(parent.attrib['ref'])
            jobs_dictionary[dependence.attrib['ref']]['unmetDependencies'].append(parent.attrib['ref'])
            jobs_dictionary[parent.attrib['ref']]['parent'].append(dependence.attrib['ref'])


    return jobs_dictionary
####buildJobsDictionary

def createBaseXML():
    return ET.Element('cgwd',attrib={'author':'parser', 'domain' : '', 'name' : 'parser-workflow', 'version':''})
####createBaseXML

def addWorkflowInputs(agwl_format,files_dictionary):
    workflow_inputs = ET.SubElement(agwl_format,'cgwdInput')
    for file in files_dictionary:
        dataIn =ET.SubElement(workflow_inputs,'dataIn')
        dataIn.attrib={'category':'Data', 'name':file, 'source':files_dictionary[file],'type':'agwl:file'}
        dataRepresentation = ET.SubElement(dataIn,'dataRepresentation')

        storageType = ET.SubElement(dataRepresentation,'storageType')
        storageType.text= 'FileSystem'
        contentType = ET.SubElement(dataRepresentation,'contentType')
        contentType.text='File'
        archiveType = ET.SubElement(dataRepresentation,'archiveType')
        archiveType.text='none'
        cardinality = ET.SubElement(dataRepresentation,'cardinality')
        cardinality.text='single'
    return agwl_format
####addWorkflowInputs

def readyToExecuteJobs(jobs_dictionary):
    independent_jobs = []
    for job in jobs_dictionary:
        if len(jobs_dictionary[job]['unmetDependencies']) == 0 and jobs_dictionary[job]['executed'] == False:
            independent_jobs.append(job)
    return independent_jobs
####readyToExecuteJobs



if __name__ == "__main__":

    ## Checking correct number of arguments
    if len(sys.argv) < 2:
        print("Usage dax2agwl [file.dax]")
        sys.exit()


    daxFile=sys.argv[1]
    # Checks the input file exists
    if not os.path.exists(daxFile):
        print("File "+daxFile+" not found")
        sys.exit()

    # Obtaining the root of the xml representing the workflow in dax format
    root = ET.parse(daxFile).getroot()

    files_dictionary = buildFilesDictionary(root.findall('{http://pegasus.isi.edu/schema/DAX}file'))
    executables_dictionary = buildExecutablesDictionary(root.findall('{http://pegasus.isi.edu/schema/DAX}executable'))
    jobs_dictionary = buildJobsDictionary(root.findall('{http://pegasus.isi.edu/schema/DAX}job'),root.findall('{http://pegasus.isi.edu/schema/DAX}child'))

    # Create an empty agwl workflow
    agwl_format = createBaseXML()

    #add inputs to a given workflow, based on the files_dictionary
    agwl_format = addWorkflowInputs(agwl_format,files_dictionary)

    # getting the tasks to be executed
    body = ET.SubElement(agwl_format,'cgwdBody')



    independent_jobs = readyToExecuteJobs(jobs_dictionary)
    fork_counter = 1
    while len(independent_jobs) > 0 :

        parallel_mode = False
        if len(independent_jobs) > 1:
            parallel_mode = True


        parallelBody = None
        if parallel_mode:
            parallel = ET.SubElement(body,'parallel')
            parallel.attrib={'name':'ForkNode_'+str(fork_counter)}
            parallelBody = ET.SubElement(parallel,'parallelBody')
            fork_counter = fork_counter + 1

        while len(independent_jobs) > 0 :
            job = independent_jobs[0]
            independent_jobs.remove(job)

            element = body
            if parallel_mode:
                section = ET.SubElement(parallelBody,'section')
                element = section

            activity = ET.SubElement(element,'activity')
            activity.attrib = {'function':'Function','name':job,'type':'soy:'+job}

            # inputs are based on dependencies
            dataIns = ET.SubElement(activity,'dataIns')
            for input_file in jobs_dictionary[job]['inputs']:
                dataIn = ET.SubElement(dataIns,'dataIn')
                if input_file in files_dictionary:
                    dataIn.attrib={'category':'Data','name':input_file,'source':'parser-workflow'+'/'+input_file,'type':'agwl:file'}
                else:
                    for parent in jobs_dictionary[job]['depends'] :
                        if input_file in jobs_dictionary[parent]['outputs']:
                            dataIn.attrib={'category':'Data','name':input_file,'source':parent+'/' +input_file,'type':'agwl:file'}
                            break

                dataRepresentation = ET.SubElement(dataIn,'dataRepresentation')
                storageType = ET.SubElement(dataRepresentation,'storageType')
                storageType.text= 'FileSystem'
                contentType = ET.SubElement(dataRepresentation,'contentType')
                contentType.text='File'
                archiveType = ET.SubElement(dataRepresentation,'archiveType')
                archiveType.text='none'
                cardinality = ET.SubElement(dataRepresentation,'cardinality')
                cardinality.text='single'

            #outputs of the task
            dataOuts = ET.SubElement(activity,'dataOuts')

            for output_file in jobs_dictionary[job]['outputs']:
                dataOut = ET.SubElement(dataOuts,'dataOut')
                dataOut.attrib = {'category':'', 'name':output_file,'saveto':'', 'type':'agwl:file'}
                dataRepresentation = ET.SubElement(dataOut,'dataRepresentation')
                storageType = ET.SubElement(dataRepresentation,'storageType')
                storageType.text= 'FileSystem'
                contentType = ET.SubElement(dataRepresentation,'contentType')
                contentType.text='File'
                archiveType = ET.SubElement(dataRepresentation,'archiveType')
                archiveType.text='none'
                cardinality = ET.SubElement(dataRepresentation,'cardinality')
                cardinality.text='single'

            jobs_dictionary[job]['executed'] = True

            for child in jobs_dictionary[job]['parent']:
                jobs_dictionary[child]['unmetDependencies'].remove(job)



        independent_jobs = readyToExecuteJobs(jobs_dictionary)

    last_activity = jobs_dictionary.keys()[-1]
    workflow_output = ET.SubElement(agwl_format,'cgwdOutput')
    for output_file in jobs_dictionary[last_activity]['outputs']:
            dataOut = ET.SubElement(workflow_output,'dataOut')
            dataOut.attrib = {'category':'', 'name':output_file,'saveto':'', 'source':last_activity+'/'+output_file,'type':'agwl:file'}
            dataRepresentation = ET.SubElement(dataOut,'dataRepresentation')
            storageType = ET.SubElement(dataRepresentation,'storageType')
            storageType.text= 'FileSystem'
            contentType = ET.SubElement(dataRepresentation,'contentType')
            contentType.text='File'
            archiveType = ET.SubElement(dataRepresentation,'archiveType')
            archiveType.text='none'
            cardinality = ET.SubElement(dataRepresentation,'cardinality')
            cardinality.text='single'



    #generation of the gwld file
    print('application = soybean')
    print('soybean.type = soy')
    print('soybean.domain = medicine')
    print('soybean.environment = ssh')
    print('DELIM = AND')
    print('')
    activities_string = 'soybean.activities='
    for job in  jobs_dictionary:
        activities_string += 'soy\\:'+job+" "
    print(activities_string)


    for job in  jobs_dictionary:
        print('soy\\:'+job+'.executable='+ntpath.basename(executables_dictionary[jobs_dictionary[job]['executable']]))
        usage_string = 'soy:\\'+job+'.usage='

        for arg in jobs_dictionary[job]['arguments']:
            usage_string += ''.join(arg)
        print(usage_string)

        input_ports = 'soy\\:'+job+'.inports='
        for file in jobs_dictionary[job]['inputs'][:-1]:
            input_ports += file+' '+file+' agwl:file'+' AND \\\n'

        if len(jobs_dictionary[job]['inputs']) > 1 :
            input_ports += jobs_dictionary[job]['inputs'][-1] +' '+jobs_dictionary[job]['inputs'][-1]+' agwl:file'
        print(input_ports)

        output_ports = 'soy\\:'+job+'.outports='
        for file in jobs_dictionary[job]['outputs'][:-1]:
            output_ports += file+' '+file+' agwl:file'+' AND \\\n'

        if len(jobs_dictionary[job]['outputs']) > 1 :
            output_ports += jobs_dictionary[job]['outputs'][-1] +' '+ jobs_dictionary[job]['outputs'][-1]+' agwl:file'
        print(output_ports)
