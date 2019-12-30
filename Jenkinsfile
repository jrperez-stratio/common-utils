@Library('libpipelines@master') _

hose {
    EMAIL = 'qa'
    MODULE = 'common-utils'
    DEVTIMEOUT = 30
    RELEASETIMEOUT = 30
    FOSS = true
    REPOSITORY = 'common-utils'
    DEPLOYONPRS = true
    RUN_TESTS = false
    CROSSBUILD = ['scala-2.11']
    
    DEV = { config ->
    
        doCompile(conf: config, crossbuild: config.CROSSBUILD[0])
        doUT(conf: config, crossbuild: config.CROSSBUILD[0])
        doPackage(conf: config, crossbuild: config.CROSSBUILD[0])

        parallel(DOC: {
            doDoc(conf: config, crossbuild: config.CROSSBUILD[0])
        }, QC: {
            doStaticAnalysis(conf: config, crossbuild: config.CROSSBUILD[0])
        }, DEPLOY: {
            doDeploy(conf: config, crossbuild: config.CROSSBUILD[0])
        }, failFast: config.FAILFAST)

    }
}

