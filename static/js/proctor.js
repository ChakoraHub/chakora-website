/*
=============================================================
Online Proctored Exam System - Client-Side Proctoring Engine
=============================================================
FIXES APPLIED:
  - Bug #2: requestWebcamPermission() exported for pre-fullscreen call
  - Bug #3: Named beforeunload handler + window.removeBeforeUnloadHandler() exported
  - Bug #4: isExamSubmitting flag check in fullscreenchange handler
  - Bug #6: MediaRecorder fallback correctly re-attaches handlers after catch
*/

// Global variables initialized by exam_handler
window.attemptId = null; 
let webcamStream = null;
let mediaRecorder = null;
let recordedChunks = [];

// FIX #2: Pre-flight webcam permission request.
// Called BEFORE requesting fullscreen so the browser permission prompt
// can be shown to the user. Returns a Promise.
window.requestWebcamPermission = function() {
    return navigator.mediaDevices.getUserMedia({ video: true, audio: false })
        .then(stream => {
            // Permission granted — store the stream immediately so startWebcamMonitoring
            // can reuse it without triggering a second permission prompt.
            window._preflightStream = stream;
            console.log("Webcam pre-flight permission granted.");
        })
        .catch(err => {
            console.warn("Webcam pre-flight permission denied:", err);
            window._preflightStream = null;
            // Re-throw so exam_handler can decide whether to block or continue
            throw err;
        });
};

// MOCK_TEST mode: start webcam recording ONLY — no proctoring violation hooks.
// Called by exam_handler.js when operationalMode === "MOCK_TEST".
// S3 upload still happens on submit via stopExamRecording() — same path as CERTIFICATION.
window.startWebcamRecordingOnly = function(attemptId) {
    window.attemptId = attemptId;
    startWebcamMonitoring();
    makeWidgetDraggable(document.getElementById("proctorWebcamWidget"), document.getElementById("proctorWebcamDragHandle"));
    console.log("Mock Test webcam recording started. No proctoring hooks active.");
};

// Initialize Proctoring Hooks once attempt starts (CERTIFICATION only)
function initProctoring(attemptId) {
    window.attemptId = attemptId;
    
    setupTabSwitchDetection();
    setupWindowFocusDetection();
    setupInteractionBlockers();
    setupFullscreenTracker();
    setupBeforeUnloadWarning();
    
    startWebcamMonitoring();
    
    makeWidgetDraggable(document.getElementById("proctorWebcamWidget"), document.getElementById("proctorWebcamDragHandle"));
}

// -------------------------------------------------------------
// Telemetry and Logging Functions
// -------------------------------------------------------------
function logViolation(type, description) {
    if (!window.attemptId) return;

    fetch(`/api/proxy/attempts/${window.attemptId}/violations`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            violation_type: type,
            description: description
        })
    })
    .then(res => {
        if (!res.ok) {
            console.error("Failed to upload proctor telemetry event");
        }
    })
    .catch(err => console.error("Proctor service connectivity error:", err));

    showViolationToast(type, description);
}

window.logProctorViolation = logViolation;

function showViolationToast(type, description) {
    const container = document.getElementById("violationToastContainer");
    if (!container) return;

    const toast = document.createElement("div");
    toast.className = "violation-toast";
    toast.innerHTML = `
        <div style="font-size: 1.5rem;"><i class="bi bi-shield-exclamation text-white"></i></div>
        <div>
            <strong style="display: block; font-size: 0.95rem; text-transform: uppercase;">Security Alert</strong>
            <span style="font-size: 0.85rem; opacity: 0.9;">${description}</span>
        </div>
    `;

    container.appendChild(toast);

    setTimeout(() => {
        toast.style.animation = "slideDown 0.4s reverse forwards";
        setTimeout(() => toast.remove(), 400);
    }, 5000);
}

// -------------------------------------------------------------
// Interactive Monitors
// -------------------------------------------------------------

function setupTabSwitchDetection() {
    document.addEventListener("visibilitychange", () => {
        // FIX #4: Suppress during intentional submission
        if (window.isExamSubmitting) return;
        if (document.hidden) {
            logViolation("tab_switch", "User navigated away from the exam tab / switched browser tab.");
        }
    });
}

function setupWindowFocusDetection() {
    window.addEventListener("blur", () => {
        // FIX #4: Suppress during intentional submission
        if (window.isExamSubmitting) return;
        logViolation("window_blur", "User lost focus on the exam window. Browser minimized or application switch detected.");
    });
    
    window.addEventListener("focus", () => {
        console.log("Exam window focus restored.");
    });
}

function setupInteractionBlockers() {
    document.addEventListener("copy", (e) => {
        e.preventDefault();
        logViolation("copy_paste_attempt", "Blocked attempt to copy content.");
    });
    document.addEventListener("paste", (e) => {
        e.preventDefault();
        logViolation("copy_paste_attempt", "Blocked attempt to paste content.");
    });
    document.addEventListener("cut", (e) => {
        e.preventDefault();
        logViolation("copy_paste_attempt", "Blocked attempt to cut content.");
    });
    document.addEventListener("contextmenu", (e) => {
        e.preventDefault();
        logViolation("right_click", "Blocked right-click context menu access.");
    });
}

// FIX #4: Check isExamSubmitting before logging fullscreen_exit.
// exitFullscreen() during submission triggers this event — it must be suppressed.
function setupFullscreenTracker() {
    document.addEventListener("fullscreenchange", () => {
        if (!document.fullscreenElement) {
            if (window.isExamSubmitting) {
                console.log("Fullscreen exited during submission — suppressing violation log.");
                return;
            }
            logViolation("fullscreen_exit", "User exited full-screen mode. Full screen is required to continue.");
        }
    });
}

// FIX #3: Use a named function reference so it can be removed later.
// window.onbeforeunload = null does NOT remove listeners added via addEventListener.
// exam_handler.js calls window.removeBeforeUnloadHandler() before redirect.
function _beforeUnloadHandler(e) {
    // Suppress if exam is being submitted
    if (window.isExamSubmitting) return;
    logViolation("page_refresh_attempt", "User attempted to refresh or navigate away from the running exam session.");
    const confirmationMessage = "Are you sure you want to exit? Your exam progress will be locked and saved as is.";
    (e || window.event).returnValue = confirmationMessage;
    return confirmationMessage;
}

function setupBeforeUnloadWarning() {
    window.addEventListener("beforeunload", _beforeUnloadHandler);
}

// FIX #3: Exported removal function so exam_handler.js can cleanly unregister
window.removeBeforeUnloadHandler = function() {
    window.removeEventListener("beforeunload", _beforeUnloadHandler);
    console.log("beforeunload proctoring handler removed.");
};

// -------------------------------------------------------------
// Webcam Feed and Disconnect Telemetry
// -------------------------------------------------------------
function startWebcamMonitoring() {
    const video = document.getElementById("proctorVideo");
    const widget = document.getElementById("proctorWebcamWidget");
    
    if (!video) return;

    // FIX #2: Reuse the pre-flight stream if available — avoids second permission prompt
    const streamPromise = window._preflightStream
        ? Promise.resolve(window._preflightStream)
        : navigator.mediaDevices.getUserMedia({ video: true, audio: false });

    // Clear pre-flight reference after consuming
    window._preflightStream = null;

    streamPromise
        .then(stream => {
            webcamStream = stream;
            video.srcObject = stream;
            
            recordedChunks = [];

            // FIX #6: Define a reusable function to attach handlers to any recorder instance
            function attachRecorderHandlers(recorder) {
                recorder.ondataavailable = (event) => {
                    if (event.data && event.data.size > 0) {
                        recordedChunks.push(event.data);
                    }
                };
                recorder.onstart = () => {
                    console.log("Webcam session recording started.");
                    updateRecordingIndicator(true);
                };
            }

            try {
                mediaRecorder = new MediaRecorder(stream, { mimeType: 'video/webm;codecs=vp8' });
                attachRecorderHandlers(mediaRecorder);
            } catch (e) {
                console.warn("video/webm;codecs=vp8 not supported, using browser default", e);
                try {
                    mediaRecorder = new MediaRecorder(stream);
                    // FIX #6: Re-attach handlers to the new fallback instance
                    attachRecorderHandlers(mediaRecorder);
                } catch (e2) {
                    console.error("MediaRecorder API not supported", e2);
                    mediaRecorder = null;
                }
            }

            if (mediaRecorder) {
                mediaRecorder.start(1000); // 1-second timeslices
            }
            
            const videoTrack = stream.getVideoTracks()[0];
            if (videoTrack) {
                videoTrack.onended = () => {
                    widget.classList.add("disconnected");
                    logViolation("webcam_disconnected", "Hardware connection lost. Webcam video feed was disconnected.");
                    updateRecordingIndicator(false);
                };
            }
            
            navigator.mediaDevices.addEventListener("devicechange", checkCameraConnectivity);
        })
        .catch(err => {
            widget.classList.add("disconnected");
            console.error("Webcam access error:", err);
            
            let desc = "Webcam access was denied by user preferences.";
            if (err.name === "NotFoundError" || err.name === "DevicesNotFoundError") {
                desc = "No camera hardware detected on device.";
            }
            
            logViolation("webcam_denied", desc);
            updateRecordingIndicator(false);
        });
}

function checkCameraConnectivity() {
    navigator.mediaDevices.enumerateDevices()
        .then(devices => {
            const hasVideoInput = devices.some(device => device.kind === 'videoinput');
            const widget = document.getElementById("proctorWebcamWidget");
            
            if (!hasVideoInput) {
                widget.classList.add("disconnected");
                logViolation("webcam_disconnected", "System changes detected: Camera hardware was detached.");
                updateRecordingIndicator(false);
            }
        })
        .catch(err => console.error("Error enumerating devices:", err));
}

function updateRecordingIndicator(isRecording) {
    const overlay = document.querySelector(".proctor-webcam-widget .widget-overlay");
    if (!overlay) return;
    
    const widget = document.getElementById("proctorWebcamWidget");
    const isDisconnected = widget && widget.classList.contains("disconnected");
    
    if (isDisconnected) {
        overlay.innerHTML = `
            <span class="status-dot" style="background-color: var(--accent-red); animation: none;"></span>
            <span class="text-white fw-bold uppercase font-monospace" style="letter-spacing: 0.05em;">OFFLINE</span>
        `;
    } else if (isRecording) {
        overlay.innerHTML = `
            <span class="status-dot" style="background-color: var(--accent-red); animation: pulse 1s infinite ease-in-out;"></span>
            <span class="text-white fw-bold uppercase font-monospace" style="letter-spacing: 0.05em;">• REC</span>
        `;
    } else {
        overlay.innerHTML = `
            <span class="status-dot" style="background-color: var(--accent-green); animation: pulse 1.5s infinite ease-in-out;"></span>
            <span class="text-white fw-bold uppercase font-monospace" style="letter-spacing: 0.05em;">PROCTOR ACTIVE</span>
        `;
    }
}

window.stopExamRecording = function() {
    return new Promise((resolve) => {
        if (!mediaRecorder || mediaRecorder.state === "inactive") {
            // MediaRecorder never started or already stopped — no blob available
            if (webcamStream) {
                webcamStream.getTracks().forEach(track => track.stop());
            }
            console.warn("stopExamRecording: mediaRecorder inactive — resolving null.");
            resolve(null);
            return;
        }

        // ROOT CAUSE FIX 1:
        // The original code called webcamStream.getTracks().forEach(track.stop()) BEFORE
        // the onstop callback fired. Stopping the underlying MediaStreamTrack while the
        // MediaRecorder is still processing causes the browser to:
        //   (a) discard any buffered data not yet delivered via ondataavailable, AND
        //   (b) fire onstop with recordedChunks still empty or incomplete.
        // Result: new Blob([], {type:'video/webm'}) — a 0-byte blob.
        // The fetch upload still runs (blob.size === 0 does not throw), but S3 receives
        // an empty file, and the Flask 60-second timeout may cancel it silently.
        //
        // FIX: Move webcamStream.getTracks().stop() calls INSIDE onstop, AFTER the
        // Blob is assembled. onstop fires only after the final ondataavailable has
        // delivered all remaining buffered data — at that point it is safe to stop tracks.

        mediaRecorder.onstop = () => {
            console.log("MediaRecorder recording stopped. Chunks collected:", recordedChunks.length);
            updateRecordingIndicator(false);

            // Assemble blob FIRST — all chunks are guaranteed delivered by this point
            const blob = new Blob(recordedChunks, { type: 'video/webm' });
            console.log("Recording blob assembled. Size:", blob.size, "bytes");

            // NOW safe to stop the underlying media tracks
            if (webcamStream) {
                webcamStream.getTracks().forEach(track => track.stop());
            }

            if (blob.size === 0) {
                console.warn("stopExamRecording: Blob is 0 bytes — no data was recorded. Resolving null.");
                resolve(null);
            } else {
                resolve(blob);
            }
        };

        try {
            // Request final data flush before stop — ensures last timeslice is delivered
            // to ondataavailable before onstop fires
            if (mediaRecorder.state === "recording") {
                mediaRecorder.requestData(); // flush any buffered data immediately
            }
            mediaRecorder.stop();
        } catch (e) {
            console.error("Failed to stop MediaRecorder:", e);
            // Stop tracks anyway to release camera indicator
            if (webcamStream) {
                webcamStream.getTracks().forEach(track => track.stop());
            }
            resolve(null);
        }

        // DO NOT stop webcamStream here — moved inside onstop above
    });
};

function stopWebcamMonitoring() {
    if (webcamStream) {
        webcamStream.getTracks().forEach(track => track.stop());
    }
}
window.stopWebcamMonitoring = stopWebcamMonitoring;

// -------------------------------------------------------------
// Floating Draggable Mechanics
// -------------------------------------------------------------
function makeWidgetDraggable(element, handle) {
    if (!element || !handle) return;
    
    let pos1 = 0, pos2 = 0, pos3 = 0, pos4 = 0;
    
    handle.onmousedown = dragMouseDown;

    function dragMouseDown(e) {
        e = e || window.event;
        e.preventDefault();
        pos3 = e.clientX;
        pos4 = e.clientY;
        document.onmouseup = closeDragElement;
        document.onmousemove = elementDrag;
    }

    function elementDrag(e) {
        e = e || window.event;
        e.preventDefault();
        pos1 = pos3 - e.clientX;
        pos2 = pos4 - e.clientY;
        pos3 = e.clientX;
        pos4 = e.clientY;
        
        let newTop = element.offsetTop - pos2;
        let newLeft = element.offsetLeft - pos1;
        
        const maxTop = window.innerHeight - element.offsetHeight - 10;
        const maxLeft = window.innerWidth - element.offsetWidth - 10;
        
        if (newTop < 10) newTop = 10;
        if (newTop > maxTop) newTop = maxTop;
        if (newLeft < 10) newLeft = 10;
        if (newLeft > maxLeft) newLeft = maxLeft;
        
        element.style.top = newTop + "px";
        element.style.left = newLeft + "px";
        element.style.bottom = "auto";
        element.style.right = "auto";
    }

    function closeDragElement() {
        document.onmouseup = null;
        document.onmousemove = null;
    }
}
