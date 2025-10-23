/*
 * Copyright 2013-2024, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.executor
import java.nio.file.Path
import java.nio.file.Paths

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.processor.TaskRun
/**
 * HTCondor executor
 *
 * See https://research.cs.wisc.edu/htcondor/
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class CondorExecutor extends AbstractGridExecutor {

    static final public String CMD_CONDOR = '.command.condor'

    protected Path resolveSubmitFilePath(TaskRun task) {
        final submitBaseDir = Paths.get('.nextflow', 'condor-submit')
        final sessionDir = submitBaseDir.resolve(session.uniqueId.toString())
        final taskHash = task.workDir.name
        return sessionDir.resolve("${taskHash}.condor")
    }

    final protected BashWrapperBuilder createBashWrapperBuilder(TaskRun task) {
        final builder = new CondorWrapperBuilder(task, this)
        builder.manifest = getDirectivesText(task)
        return builder
    }

    protected String getDirectivesText(TaskRun task) {
        def lines = getDirectives(task)
        lines << ''
        lines.join('\n')
    }

    @Override
    protected String getHeaderToken() {
        throw new UnsupportedOperationException()
    }

    @Override
    protected List<String> getDirectives(TaskRun task, List<String> result) {

        result << "universe = vanilla"
        result << "executable = ${task.workDir.resolve(TaskRun.CMD_RUN)}".toString()
        result << "log = ${TaskRun.CMD_LOG}".toString()
        result << "getenv = true"

        if( task.config.getCpus()>1 ) {
            result << "request_cpus = ${task.config.getCpus()}".toString()
            result << "machine_count = 1"
        }

        if( task.config.getMemory() ) {
            result << "request_memory = ${task.config.getMemory()}".toString()
        }

        if( task.config.getDisk() ) {
            result << "request_disk = ${task.config.getDisk()}".toString()
        }

        if( task.config.getTime() ) {
            result << "periodic_remove = (RemoteWallClockTime - CumulativeSuspensionTime) > ${task.config.getTime().toSeconds()}".toString()
        }

        if( task.config.getClusterOptions() ) {
            def opts = task.config.getClusterOptions()
            if( opts instanceof Collection ) {
                result.addAll(opts as Collection)
            }
            else {
                result.addAll( opts.toString().tokenize(';\n').collect{ it.trim() })
            }
        }

        result<< "queue"

    }

    @Override
    List<String> getSubmitCommandLine(TaskRun task, Path scriptFile) {
        final condorFile = resolveSubmitFilePath(task)
        return ['condor_submit', '--terse', condorFile.toString()]
    }

    @Override
    def parseJobId(String text) {
        text.tokenize(' -')[0]
    }

    @Override
    protected List<String> getKillCommand() {
        ['condor_rm']
    }

    @Override
    protected List<String> queueStatusCommand(Object queue) {
        ["condor_q", "-nobatch"]
    }


    static protected Map<String,QueueStatus> DECODE_STATUS = [
            'U': QueueStatus.PENDING,   // Unexpanded
            'I': QueueStatus.PENDING,   // Idle
            'R': QueueStatus.RUNNING,   // Running
            'X': QueueStatus.ERROR,     // Removed
            'C': QueueStatus.DONE,      // Completed
            'H': QueueStatus.HOLD,      // Held
            'E': QueueStatus.ERROR      // Error
    ]


    @Override
    protected Map<String, QueueStatus> parseQueueStatus(String text) {
        final result = new LinkedHashMap<String, QueueStatus>()
        if( !text ) return result

        boolean started = false
        def itr = text.readLines().iterator()
        while( itr.hasNext() ) {
            String line = itr.next()
            if( !started ) {
                started = line.startsWith(' ID ')
                continue
            }

            if( !line.trim() ) {
                break
            }

            def cols = line.tokenize(' ')
            def id = cols[0]
            def st = cols[5]
            result[id] = DECODE_STATUS[st]
        }

        return result
    }

    @Override
    void shutdown() {
        super.shutdown()
        if( session.config.cleanup )
            cleanupSubmitFiles()
    }

    protected void cleanupSubmitFiles() {
        final submitBaseDir = Paths.get('.nextflow', 'condor-submit')
        final sessionDir = submitBaseDir.resolve(session.uniqueId.toString())
        
        if( sessionDir.exists() ) {
            log.debug "[CONDOR] Cleaning up submit files in: $sessionDir"
            try {
                sessionDir.deleteDir()
            } catch( Exception e ) {
                log.warn "[CONDOR] Failed to cleanup submit files: ${e.message}"
            }
        }
    }


    static class CondorWrapperBuilder extends BashWrapperBuilder {

        String manifest
        CondorExecutor executor
        TaskRun task

        CondorWrapperBuilder(TaskRun task, CondorExecutor executor) {
            super(task)
            this.executor = executor
            this.task = task
        }

        Path build() {
            final wrapper = super.build()
            wrapper.setExecutable(true)
            
            final Path condorFile = executor.resolveSubmitFilePath(task)
            
            if( condorFile.parent != this.workDir ) {
                condorFile.parent.toFile().mkdirs()
            }
            
            condorFile.text = manifest
            return wrapper
        }

    }
}
