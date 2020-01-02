@Library('libpipelines@master') _

hose {
    EMAIL = 'qa'
    DEVTIMEOUT = 30
    RELEASETIMEOUT = 30
    DEPLOYONPRS = true
    RUN_TESTS = false
    DEV = { config ->
    
        doCompile(conf: config)
        doUT(conf: config)
        doPackage(conf: config)

        parallel(DOC: {
            doDoc(conf: config)
        }, QC: {
            doStaticAnalysis(conf: config)
        }, DEPLOY: {
            doDeploy(conf: config)
        }, failFast: config.FAILFAST)

    }
}

