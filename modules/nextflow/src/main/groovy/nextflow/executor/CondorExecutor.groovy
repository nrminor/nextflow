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
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.processor.TaskBean
import nextflow.processor.TaskRun
import nextflow.util.Escape
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

    private Path stagedBinDir

    protected Path resolveSubmitFilePath(TaskRun task) {
        final submitDir = config?.getExecConfigProp(name, 'submitFileDir', null) as String
        
        if( submitDir ) {
            final submitBaseDir = Paths.get(submitDir)
            final taskHash = task.workDir.name
            final prefix = taskHash.substring(0, 2)
            return submitBaseDir.resolve(prefix).resolve("${taskHash}.condor")
        } else {
            return task.workDir.resolve(CMD_CONDOR)
        }
    }

    protected Path resolveLogFilePath(TaskRun task) {
        final submitDir = config?.getExecConfigProp(name, 'submitFileDir', null) as String
        
        if( submitDir ) {
            final submitBaseDir = Paths.get(submitDir)
            final taskHash = task.workDir.name
            final prefix = taskHash.substring(0, 2)
            return submitBaseDir.resolve(prefix).resolve("${taskHash}.log")
        } else {
            return task.workDir.resolve(TaskRun.CMD_LOG)
        }
    }

    final protected BashWrapperBuilder createBashWrapperBuilder(TaskRun task) {
        // Get path mappings from config
        final pathMappings = config?.getExecConfigProp(name, 'pathMappings', null) as Map<String,String>
        
        // Create TaskBean once to avoid multiple conversions
        final bean = new TaskBean(task)
        
        // Create appropriate file copy strategy
        final copyStrategy = pathMappings
            ? new CondorFileCopyStrategy(bean, pathMappings)
            : new SimpleFileCopyStrategy(bean)
        
        // Create wrapper builder with custom strategy
        final builder = new CondorWrapperBuilder(bean, task, this, copyStrategy)
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
        final logPath = resolveLogFilePath(task)
        result << "log = ${logPath}".toString()
        final defaultGetenv = isSharedFilesystem()
        final getenv = config?.getExecConfigProp(name, 'getenv', defaultGetenv) as boolean
        if( getenv ) {
            result << "getenv = true"
        }

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
        final submitDir = config?.getExecConfigProp(name, 'submitFileDir', null) as String
        
        if( submitDir ) {
            final submitBaseDir = Paths.get(submitDir)
            
            if( submitBaseDir.exists() ) {
                log.debug "[CONDOR] Cleaning up submit files in: $submitBaseDir"
                try {
                    submitBaseDir.deleteDir()
                } catch( Exception e ) {
                    log.warn "[CONDOR] Failed to cleanup submit files: ${e.message}"
                }
            }
        }
    }

    protected boolean isSharedFilesystem() {
        config?.getExecConfigProp(name, 'sharedFilesystem', true) as boolean
    }

    @Override
    void register() {
        super.register()
        
        if( !isSharedFilesystem() ) {
            final submitDir = config?.getExecConfigProp(name, 'submitFileDir', null)
            
            if( !submitDir ) {
                final errorMsg = """\
                    HTCondor executor configuration error:
                    
                    When sharedFilesystem = false, submitFileDir must be specified.
                    
                    Example configuration:
                    executor {
                        \$condor {
                            sharedFilesystem = false
                            submitFileDir = '.nextflow/condor-submit'
                            pathMappings = [
                                '/mnt/htc-cephfs/fuse/root/staging': '/staging'
                            ]
                        }
                    }
                    
                    Note: The \$ prefix is required for executor-specific configuration.
                    
                    When sharedFilesystem = false:
                    - Input files will be staged with normalized paths (if pathMappings configured)
                    - Output files will be copied back from execution sandbox to work directory
                    - Control files (.command.out, .command.err, .command.trace) will be copied back
                    - This is required for environments like CHTC where compute nodes run
                      in isolated sandboxes with different filesystem mount topologies
                      
                    The pathMappings option normalizes canonical filesystem paths to container-visible
                    mount points. For example, if /staging is a symlink to /mnt/htc-cephfs/fuse/root/staging
                    on the submit node, but only /staging is visible in containers, configure:
                        pathMappings = ['/mnt/htc-cephfs/fuse/root/staging': '/staging']
                    """.stripIndent()
                
                log.error errorMsg
                throw new IllegalArgumentException("submitFileDir is required when sharedFilesystem = false")
            }
            
            final pathMappings = config?.getExecConfigProp(name, 'pathMappings', null)
            
            log.debug "[CONDOR] Running in restricted filesystem mode - submit files: ${submitDir}"
            log.debug "[CONDOR] pathMappings raw value: ${pathMappings}, type: ${pathMappings?.getClass()?.name}"
            
            if( pathMappings && pathMappings instanceof Map ) {
                final mappingsMap = pathMappings as Map<String,String>
                if( mappingsMap && !mappingsMap.isEmpty() ) {
                    log.debug "[CONDOR] Path mappings configured:"
                    mappingsMap.each { canonical, alias ->
                        log.debug "[CONDOR]   ${canonical} -> ${alias}"
                    }
                } else {
                    log.debug "[CONDOR] Path mappings is empty map"
                }
            } else {
                log.debug "[CONDOR] No path mappings configured (value: ${pathMappings}) - canonical paths will NOT be normalized"
            }
            log.debug "[CONDOR] Input staging: ${pathMappings ? 'symlink (with path normalization)' : 'symlink'}"
            log.debug "[CONDOR] Output unstaging: enabled"
            log.debug "[CONDOR] HTCondor event logs disabled in restricted filesystem mode"
        } else {
            log.debug "[CONDOR] Running in shared filesystem mode"
        }
    }

    @Override
    Path getBinDir() {
        final explicit = config?.getExecConfigProp(name, 'stageBinDir', null)
        
        final shouldStage = (explicit == null || explicit == 'auto')
            ? !isSharedFilesystem()
            : (explicit as boolean)
        
        if( !shouldStage ) {
            return session.getBinDir()
        }
        
        final projectBinDir = session.getBinDir()
        if( !projectBinDir ) {
            return null
        }
        
        if( stagedBinDir ) {
            return stagedBinDir
        }
        
        synchronized(this) {
            if( stagedBinDir ) {
                return stagedBinDir
            }
            
            final targetDir = workDir.resolve('.nextflow-bin')
            
            try {
                Files.createDirectories(targetDir)
                
                projectBinDir.eachFile { file ->
                    final target = targetDir.resolve(file.name)
                    if( file.isDirectory() ) {
                        file.copyTo(target)
                    } else {
                        Files.copy(file, target, StandardCopyOption.REPLACE_EXISTING)
                        target.setPermissions(file.getPermissions())
                    }
                }
                
                log.debug "[CONDOR] Staged bin directory from ${projectBinDir} to ${targetDir}"
                stagedBinDir = targetDir
                
            } catch( Exception e ) {
                log.warn "[CONDOR] Failed to stage bin directory: ${e.message}"
                return projectBinDir
            }
            
            return stagedBinDir
        }
    }


    static class CondorWrapperBuilder extends BashWrapperBuilder {

        String manifest
        CondorExecutor executor
        TaskRun task

        CondorWrapperBuilder(TaskBean bean, TaskRun task, CondorExecutor executor, ScriptFileCopyStrategy copyStrategy = null) {
            super(bean, copyStrategy)
            this.executor = executor
            this.task = task
        }

        @Override
        protected boolean shouldUnstageControls() {
            final explicit = executor.config?.getExecConfigProp(executor.name, 'unstageOutputs', null)
            
            if( explicit != null && explicit != 'auto' ) {
                return explicit as boolean
            }
            
            return !executor.isSharedFilesystem()
        }

        @Override
        protected boolean shouldUnstageOutputs() {
            final explicit = executor.config?.getExecConfigProp(executor.name, 'unstageOutputs', null)
            
            if( explicit != null && explicit != 'auto' ) {
                return explicit as boolean
            }
            
            // Auto mode: unstage outputs when not using shared filesystem
            return !executor.isSharedFilesystem()
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

    /**
     * Custom file copy strategy for HTCondor that supports path normalization
     * to handle containers with different filesystem mount topologies.
     */
    static class CondorFileCopyStrategy extends SimpleFileCopyStrategy {
        
        private Map<String,String> pathMappings
        
        CondorFileCopyStrategy(TaskBean bean, Map<String,String> pathMappings) {
            super(bean)
            this.pathMappings = pathMappings ?: [:]
        }
        
        @Override
        String stageInputFile(Path path, String targetName) {
            def cmd = ''
            def p = targetName.lastIndexOf('/')
            if( p>0 ) {
                cmd += "mkdir -p ${Escape.path(targetName.substring(0,p))} && "
            }
            
            // Get absolute path (this handles relative paths if any)
            def pathStr = path.toAbsolutePath().toString()
            
            // Apply path mappings to normalize canonical paths
            pathStr = normalizePath(pathStr)
            
            // Generate staging command with normalized path
            cmd += stageInCommand(pathStr, targetName, stageinMode)
            return cmd
        }
        
        /**
         * Normalize a path by applying configured path mappings.
         * Uses longest-match-first algorithm to handle nested mappings.
         * 
         * @param path The canonical path to normalize
         * @return The normalized path with mappings applied
         */
        protected String normalizePath(String path) {
            if( !pathMappings ) {
                return path
            }
            
            // Sort mappings by key length (longest first) to handle nested paths
            def sortedMappings = pathMappings.entrySet()
                .sort { a, b -> b.key.length() <=> a.key.length() }
            
            for( entry in sortedMappings ) {
                if( path.startsWith(entry.key) ) {
                    def normalized = entry.value + path.substring(entry.key.length())
                    log.trace "[CONDOR] Normalized path: $path -> $normalized"
                    return normalized
                }
            }
            
            return path
        }
    }
}
