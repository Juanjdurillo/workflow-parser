import xml.etree.ElementTree as ET
import collections
from xml.dom import minidom



    

if __name__ == "__main__":
    #tree = ET.parse('soykb.dax')
    tree = ET.parse('1000genome.dax')
    root = tree.getroot()
    
    files = root.findall('{http://pegasus.isi.edu/schema/DAX}file')
    executables = root.findall('{http://pegasus.isi.edu/schema/DAX}executable')
    jobs = root.findall('{http://pegasus.isi.edu/schema/DAX}job')
    dependencies = root.findall('{http://pegasus.isi.edu/schema/DAX}child')



    files_dictionary = {}
    # a file has a single element pfn with the url of the file
    for file in files:
        files_dictionary[file.attrib['name']] = file[0].attrib['url']
    
    executables_dictionary = {}
    # a executable has a single element pfn with the url of the file
    for executable in executables:
        executables_dictionary[executable.attrib['name']] = file[0].attrib['url']

    #print("List of executables")
    #print(executables_dictionary)
    jobs_dictionary = collections.OrderedDict()#{}
    for job in jobs:
        jobs_dictionary[job.attrib['id']] = {}
        # obtaining executable
        jobs_dictionary[job.attrib['id']]['executable'] = job.attrib['name']
        # obtaining arguments
        jobs_dictionary[job.attrib['id']]['arguments'] = []
        argument = job.find('{http://pegasus.isi.edu/schema/DAX}argument')
        if argument is not None:
            sub_files = argument.findall('{http://pegasus.isi.edu/schema/DAX}file')            
            if len(sub_files) == 0:
                # this is a hardcoded argument
                jobs_dictionary[job.attrib['id']]['arguments'].append(argument.text)
            else:
                i = 0
                for text in argument.itertext():
                    if i < len(sub_files):
                        jobs_dictionary[job.attrib['id']]['arguments'].append([argument.text,sub_files[i].attrib['name']])
                        i = i + 1
                    else:
                        jobs_dictionary[job.attrib['id']]['arguments'].append(text)




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
        jobs_dictionary[job.attrib['id']]['parent'] = []
        jobs_dictionary[job.attrib['id']]['executed'] = False

    #adding dependencies
    for dependence in dependencies:
        parents = dependence.findall('{http://pegasus.isi.edu/schema/DAX}parent')
        for parent in parents:
            jobs_dictionary[dependence.attrib['ref']]['depends'].append(parent.attrib['ref'])
            jobs_dictionary[parent.attrib['ref']]['parent'].append(dependence.attrib['ref'])
        
        


        
    # transforming the workflow to agwl
    agwl_format = ET.Element('cgwd',attrib={'author':'parser', 'domain' : '', 'name' : 'parser-workflow', 'version':''})
    

    #inputs of the workflow
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

    # getting the tasks to be executed
    body = ET.SubElement(agwl_format,'cgwdBody')

    #jobs_dictionary = collections.OrderedDict(sorted(jobs_dictionary.items())) # we created sorted already



    independent_jobs = []
    for job in jobs_dictionary:
        if len(jobs_dictionary[job]['depends']) == 0 and jobs_dictionary[job]['executed'] == False:
          #  print job
            independent_jobs.append(job)


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
            #print('considering',job)
            independent_jobs.remove(job)
        #for job in independent_jobs:
            
            element = body
            if parallel_mode:
                section = ET.SubElement(parallelBody,'section')
                element = section

            activity = ET.SubElement(element,'activity')
            activity.attrib = {'function':'Function','name':job,'type':job}

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
                jobs_dictionary[child]['depends'].remove(job)
            
        
        
        for job in jobs_dictionary:
            if len(jobs_dictionary[job]['depends']) == 0 and jobs_dictionary[job]['executed'] == False:
                #print(job)
                independent_jobs.append(job)
        
        

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

    

    #pretty print of the workflow
    rough_string = ET.tostring(agwl_format, 'utf-8')
    reparsed = minidom.parseString(rough_string)

    print(reparsed.toprettyxml(indent="  "))
