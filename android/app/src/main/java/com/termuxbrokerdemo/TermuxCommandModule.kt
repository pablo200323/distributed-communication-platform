package com.termuxbrokerdemo

import android.app.PendingIntent
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import com.facebook.react.bridge.Arguments
import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.bridge.ReactContextBaseJavaModule
import com.facebook.react.bridge.ReactMethod
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import com.facebook.react.modules.core.PermissionAwareActivity
import com.facebook.react.modules.core.PermissionListener

class TermuxCommandModule(
    private val reactContext: ReactApplicationContext
) : ReactContextBaseJavaModule(reactContext) {

    override fun getName(): String {
        return "TermuxCommandRunner"
    }

    @ReactMethod
    fun requestRunCommandPermission(promise: Promise) {
        try {
            if (!isPackageInstalled("com.termux")) {
                val result = Arguments.createMap()
                result.putBoolean("granted", false)
                result.putString("errorCode", "TERMUX_APP_NOT_INSTALLED")
                result.putString("errorMessage", "Termux is not installed on this device.")
                promise.resolve(result)
                return
            }

            if (!isRunCommandServiceAvailable()) {
                val result = Arguments.createMap()
                result.putBoolean("granted", false)
                result.putString("errorCode", "TERMUX_RUN_COMMAND_SERVICE_UNAVAILABLE")
                result.putString("errorMessage", "Termux RUN_COMMAND service is not available.")
                promise.resolve(result)
                return
            }

            if (isRunCommandPermissionGranted()) {
                val result = Arguments.createMap()
                result.putBoolean("granted", true)
                result.putString("errorCode", "")
                result.putString("errorMessage", "")
                promise.resolve(result)
                return
            }

            val activity = getCurrentActivity()

            if (activity == null) {
                val result = Arguments.createMap()
                result.putBoolean("granted", false)
                result.putString("errorCode", "ACTIVITY_UNAVAILABLE")
                result.putString("errorMessage", "There is no active Android Activity to request the permission.")
                promise.resolve(result)
                return
            }

            if (activity !is PermissionAwareActivity) {
                val result = Arguments.createMap()
                result.putBoolean("granted", false)
                result.putString("errorCode", "PERMISSION_AWARE_ACTIVITY_UNAVAILABLE")
                result.putString("errorMessage", "Current activity cannot request Android permissions.")
                promise.resolve(result)
                return
            }

            activity.requestPermissions(
                arrayOf("com.termux.permission.RUN_COMMAND"),
                5501,
                object : PermissionListener {
                    override fun onRequestPermissionsResult(
                        requestCode: Int,
                        permissions: Array<String>,
                        grantResults: IntArray
                    ): Boolean {
                        if (requestCode != 5501) {
                            return false
                        }

                        val granted =
                            grantResults.isNotEmpty() &&
                                grantResults[0] == PackageManager.PERMISSION_GRANTED

                        val result = Arguments.createMap()
                        result.putBoolean("granted", granted)

                        if (granted) {
                            result.putString("errorCode", "")
                            result.putString("errorMessage", "")
                        } else {
                            result.putString("errorCode", "TERMUX_RUN_COMMAND_PERMISSION_DENIED")
                            result.putString("errorMessage", "The user denied com.termux.permission.RUN_COMMAND permission.")
                        }

                        promise.resolve(result)
                        return true
                    }
                }
            )
        } catch (error: Exception) {
            promise.reject(
                "TERMUX_PERMISSION_REQUEST_FAILED",
                error.message ?: "Failed to request Termux RUN_COMMAND permission.",
                error
            )
        }
    }

    @ReactMethod
    fun checkAvailability(promise: Promise) {
        try {
            val packageManager = reactContext.packageManager

            val termuxInstalled = isPackageInstalled("com.termux")
            val runCommandServiceAvailable = isRunCommandServiceAvailable()
            val permissionGranted = isRunCommandPermissionGranted()

            val available =
                termuxInstalled &&
                    runCommandServiceAvailable &&
                    permissionGranted

            val result = Arguments.createMap()
            result.putBoolean("available", available)
            result.putBoolean("termuxInstalled", termuxInstalled)
            result.putBoolean("runCommandServiceAvailable", runCommandServiceAvailable)
            result.putBoolean("permissionGranted", permissionGranted)

            if (!termuxInstalled) {
                result.putString("errorCode", "TERMUX_APP_NOT_INSTALLED")
                result.putString("errorMessage", "Termux is not installed on this device.")
            } else if (!runCommandServiceAvailable) {
                result.putString("errorCode", "TERMUX_RUN_COMMAND_SERVICE_UNAVAILABLE")
                result.putString("errorMessage", "Termux RUN_COMMAND service is not available.")
            } else if (!permissionGranted) {
                result.putString("errorCode", "TERMUX_RUN_COMMAND_PERMISSION_DENIED")
                result.putString("errorMessage", "The app does not have com.termux.permission.RUN_COMMAND permission.")
            } else {
                result.putString("errorCode", "")
                result.putString("errorMessage", "")
            }

            promise.resolve(result)
        } catch (error: Exception) {
            promise.reject(
                "TERMUX_AVAILABILITY_CHECK_FAILED",
                error.message ?: "Failed to check Termux availability.",
                error
            )
        }
    }

    private fun isPackageInstalled(packageName: String): Boolean {
        return try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                reactContext.packageManager.getPackageInfo(
                    packageName,
                    PackageManager.PackageInfoFlags.of(0)
                )
            } else {
                @Suppress("DEPRECATION")
                reactContext.packageManager.getPackageInfo(packageName, 0)
            }

            true
        } catch (error: PackageManager.NameNotFoundException) {
            false
        }
    }

    private fun isRunCommandServiceAvailable(): Boolean {
        val intent = Intent()
        intent.setClassName("com.termux", "com.termux.app.RunCommandService")
        intent.action = "com.termux.RUN_COMMAND"

        val resolveInfo =
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                reactContext.packageManager.resolveService(
                    intent,
                    PackageManager.ResolveInfoFlags.of(0)
                )
            } else {
                @Suppress("DEPRECATION")
                reactContext.packageManager.resolveService(intent, 0)
            }

        return resolveInfo != null
    }

    private fun isRunCommandPermissionGranted(): Boolean {
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            reactContext.checkSelfPermission("com.termux.permission.RUN_COMMAND") ==
                PackageManager.PERMISSION_GRANTED
        } else {
            reactContext.packageManager.checkPermission(
                "com.termux.permission.RUN_COMMAND",
                reactContext.packageName
            ) == PackageManager.PERMISSION_GRANTED
        }
    }

    @ReactMethod
    fun run(command: String, promise: Promise) {
        val executionId = nextExecutionId.getAndIncrement()
        pendingPromises[executionId] = promise

        try {
            val resultIntent = Intent(reactContext, TermuxCommandResultService::class.java)
            resultIntent.putExtra("executionId", executionId)

            val flags =
                PendingIntent.FLAG_ONE_SHOT or
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
                        PendingIntent.FLAG_MUTABLE
                    } else {
                        0
                    }

            val pendingIntent = PendingIntent.getService(
                reactContext,
                executionId,
                resultIntent,
                flags
            )

            val intent = Intent()
            intent.setClassName("com.termux", "com.termux.app.RunCommandService")
            intent.action = "com.termux.RUN_COMMAND"

            intent.putExtra("com.termux.RUN_COMMAND_PATH", "/data/data/com.termux/files/usr/bin/bash")
            intent.putExtra("com.termux.RUN_COMMAND_ARGUMENTS", arrayOf("-lc", command))
            intent.putExtra("com.termux.RUN_COMMAND_WORKDIR", "/data/data/com.termux/files/home")
            intent.putExtra("com.termux.RUN_COMMAND_BACKGROUND", true)
            intent.putExtra("com.termux.RUN_COMMAND_PENDING_INTENT", pendingIntent)

            val startedService = reactContext.startService(intent)

            if (startedService == null) {
                pendingPromises.remove(executionId)
                promise.reject(
                    "TERMUX_RUN_COMMAND_SERVICE_NOT_STARTED",
                    "Android could not start Termux RunCommandService."
                )
            }
        } catch (error: Exception) {
            pendingPromises.remove(executionId)
            promise.reject(
                "TERMUX_COMMAND_SEND_FAILED",
                error.message ?: "Failed to send command to Termux.",
                error
            )
        }
    }

    companion object {
        private val nextExecutionId = AtomicInteger(1)
        private val pendingPromises = ConcurrentHashMap<Int, Promise>()

        fun resolveExecution(
            executionId: Int,
            stdout: String,
            stderr: String,
            exitCode: Int,
            errCode: Int,
            errmsg: String
        ) {
            val promise = pendingPromises.remove(executionId) ?: return

            val result = Arguments.createMap()
            result.putInt("executionId", executionId)
            result.putString("stdout", stdout)
            result.putString("stderr", stderr)
            result.putInt("exitCode", exitCode)
            result.putInt("errCode", errCode)
            result.putString("errmsg", errmsg)

            promise.resolve(result)
        }

        fun rejectExecution(executionId: Int, message: String) {
            val promise = pendingPromises.remove(executionId) ?: return
            promise.reject("TERMUX_COMMAND_RESULT_FAILED", message)
        }
    }
}