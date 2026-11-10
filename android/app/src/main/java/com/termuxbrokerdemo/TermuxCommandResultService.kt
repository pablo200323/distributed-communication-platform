package com.termuxbrokerdemo

import android.app.IntentService
import android.content.Intent

class TermuxCommandResultService : IntentService("TermuxCommandResultService") {

    override fun onHandleIntent(intent: Intent?) {
        if (intent == null) {
            return
        }

        val executionId = intent.getIntExtra("executionId", -1)

        if (executionId == -1) {
            return
        }

        val resultBundle = intent.getBundleExtra("result")

        if (resultBundle == null) {
            TermuxCommandModule.rejectExecution(
                executionId,
                "Termux did not return a result bundle."
            )
            return
        }

        val stdout = resultBundle.getString("stdout", "")
        val stderr = resultBundle.getString("stderr", "")
        val exitCode = resultBundle.getInt("exitCode", -1)
        val errCode = resultBundle.getInt("err", -1)
        val errmsg = resultBundle.getString("errmsg", "")

        TermuxCommandModule.resolveExecution(
            executionId,
            stdout,
            stderr,
            exitCode,
            errCode,
            errmsg
        )
    }
}