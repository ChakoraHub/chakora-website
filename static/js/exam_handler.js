/*
=============================================================
Online Proctored Exam System - Exam navigation & sync engine
=============================================================
MOCK_TEST changes in this file:
  - EXAM_TYPE constant read from template (set by Flask in exam.html)
  - renderStartScreen() shows different copy for MOCK_TEST vs CERTIFICATION
  - requestWebcamThenFullscreen(): webcam runs for BOTH modes
  - startExamAttempt(): stores exam_type from API response
  - CERTIFICATION: fullscreen enforced, proctoring hooks active, violation tracking, SES email
  - MOCK_TEST: no fullscreen, no proctoring hooks, instant answer feedback + explanation shown
  - selectOption(): in MOCK_TEST shows correct/wrong highlight + explanation immediately
  - submitExamFlow(): webcam recording + S3 upload runs for BOTH modes
  - proceedToSubmit(): MOCK_TEST redirects to result page same as CERTIFICATION
*/

let questions = [];
let currentQuestionIndex = 0;
let userAnswers = {};           // { questionId: selectedOption }
let timeRemainingSeconds = 0;
let timerInterval = null;
let examStarted = false;
let operationalMode = "CERTIFICATION"; // overwritten by API response in startExamAttempt()

// Suppress proctor events during intentional submission
window.isExamSubmitting = false;

document.addEventListener("DOMContentLoaded", () => {
    renderStartScreen();
});

// -------------------------------------------------------------
// PREMIUM TOAST & MODAL HELPERS (if not available in base)
// -------------------------------------------------------------
function showPremiumToastFallback(message, type = 'info', title = '') {
    if (typeof showPremiumToast === 'function') {
        showPremiumToast(message, type, title);
    } else {
        // Fallback to alert
        alert(message);
    }
}

function showPremiumModalFallback(message, title = 'Are you sure?', type = 'warning', confirmText = 'Confirm') {
    if (typeof showPremiumModal === 'function') {
        return showPremiumModal(message, title, type, confirmText);
    } else {
        // Fallback to confirm
        return Promise.resolve(confirm(message));
    }
}

// -------------------------------------------------------------
// Start Screen — different copy per mode
// -------------------------------------------------------------
function renderStartScreen() {
    const card = document.getElementById("questionCard");
    if (!card) return;

    const isMock = (typeof EXAM_TYPE !== "undefined" && EXAM_TYPE === "MOCK_TEST");

    const icon = isMock ? "bi-journal-bookmark-fill" : "bi-shield-lock";
    const title = isMock ? "Ready to Practice?" : "Ready to Begin?";
    const description = isMock
        ? "Practice mode: instant feedback shown after each answer. Your webcam will be recorded and proctoring is active. Do not switch tabs or cover the camera. Maximum 2 warnings are allowed before auto-submission."
        : "By clicking start, you will enter a secure fullscreen mode. Webcam access is required. All activity is monitored. Maximum 2 warnings are allowed before auto-submission.";
    const btnLabel = isMock ? "Start Practice Test" : "Begin Secure Session";

    card.innerHTML = `
        <div class="text-center py-4">
            <i class="bi ${icon} text-gradient-blue" style="font-size: 3.5rem;"></i>
            <h3 class="text-white mt-3">${title}</h3>
            <p class="text-secondary mx-auto mb-4" style="max-width: 500px;">${description}</p>
            <button class="btn btn-premium px-5 py-3 btn-lg d-flex align-items-center gap-2 mx-auto" id="launchExamBtn">
                <span>${btnLabel}</span> <i class="bi bi-arrow-right-circle-fill"></i>
            </button>
        </div>
    `;

    document.getElementById("launchExamBtn").addEventListener("click", () => {
        requestWebcamThenStart();
    });
}

// -------------------------------------------------------------
// Webcam is always requested first for BOTH modes.
// Fullscreen is only enforced for CERTIFICATION.
// -------------------------------------------------------------
function requestWebcamThenStart() {
    const card = document.getElementById("questionCard");
    card.innerHTML = `
        <div class="text-center py-5">
            <div class="spinner-border text-primary" role="status">
                <span class="visually-hidden">Loading...</span>
            </div>
            <p class="text-secondary mt-3 mb-0">Requesting webcam access...</p>
        </div>
    `;

    // Always request webcam — both CERTIFICATION and MOCK_TEST record the candidate
    const webcamPromise = (typeof window.requestWebcamPermission === "function")
        ? window.requestWebcamPermission()
        : Promise.resolve();

    webcamPromise
        .catch(err => {
            // Permission denied — log warning but continue. The recording will be null.
            console.warn("Webcam pre-flight denied, proceeding anyway:", err);
        })
        .then(() => {
            // Enforce fullscreen for both CERTIFICATION and MOCK_TEST to monitor violations
            requestFullscreenAndStart();
        });
}

function requestFullscreenAndStart() {
    const docEl = document.documentElement;
    const requestFS = docEl.requestFullscreen
        || docEl.mozRequestFullScreen
        || docEl.webkitRequestFullscreen
        || docEl.msRequestFullscreen;

    if (requestFS) {
        requestFS.call(docEl)
            .then(() => startExamAttempt())
            .catch(() => {
                showPremiumToastFallback("Security policy requires full screen to continue. Please click start again and allow fullscreen.", "warning", "Fullscreen Required");
                renderStartScreen();
            });
    } else {
        startExamAttempt();
    }
}

// -------------------------------------------------------------
// Start Attempt & Fetch Questions
// -------------------------------------------------------------
function startExamAttempt() {
    const card = document.getElementById("questionCard");
    card.innerHTML = `
        <div class="text-center py-5">
            <div class="spinner-border text-primary" role="status">
                <span class="visually-hidden">Loading...</span>
            </div>
            <p class="text-secondary mt-3 mb-0">Initializing session...</p>
        </div>
    `;

    const startPayload = {
        user_id: USER_ID,
        exam_id: EXAM_ID,
        is_custom_mock: (typeof IS_CUSTOM_MOCK !== "undefined" && IS_CUSTOM_MOCK)
    };

    fetch("/api/proxy/attempts/start", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(startPayload)
    })
        .then(res => {
            if (!res.ok) throw new Error("Could not start exam attempt");
            return res.json();
        })
        .then(data => {
            window.attemptId = data.attempt_id;
            timeRemainingSeconds = data.duration_minutes * 60;

            // Store operational mode from API — source of truth
            operationalMode = data.exam_type || "CERTIFICATION";

            // Activate proctoring and violation tracking for both CERTIFICATION and MOCK_TEST
            if (typeof initProctoring === "function") {
                initProctoring(window.attemptId);
            }

            const questionsUrl = (typeof IS_CUSTOM_MOCK !== "undefined" && IS_CUSTOM_MOCK)
                ? `/api/proxy/mock-tests/${EXAM_ID}/questions`
                : `/api/proxy/exams/${EXAM_ID}/questions`;
            return fetch(questionsUrl);
        })
        .then(res => {
            if (!res.ok) throw new Error("Could not load exam questions");
            return res.json();
        })
        .then(data => {
            questions = data;
            if (questions.length === 0) {
                card.innerHTML = `
                <div class="text-center py-4">
                    <i class="bi bi-exclamation-octagon text-danger" style="font-size: 3rem;"></i>
                    <h4 class="mt-3 text-white">No Questions Found</h4>
                    <p class="text-secondary">This exam has no questions yet. Contact your administrator.</p>
                    <button class="btn btn-premium-outline mt-3" onclick="exitAndReturn()">Exit</button>
                </div>
            `;
                return;
            }

            examStarted = true;
            startTimer();
            questions.forEach(q => { userAnswers[q.id] = null; });
            document.getElementById("prevBtn").style.display = "block";
            document.getElementById("nextBtn").style.display = "block";
            renderQuestion();
        })
        .catch(err => {
            card.innerHTML = `
            <div class="text-center py-4">
                <i class="bi bi-wifi-off text-danger" style="font-size: 3rem;"></i>
                <h4 class="mt-3 text-white">Connection Error</h4>
                <p class="text-secondary">${escapeHTML(err.message || "Failed to initialize exam.")}</p>
                <button class="btn btn-premium mt-3" onclick="location.reload()">Retry</button>
            </div>
        `;
        });
}

// -------------------------------------------------------------
// Timer
// -------------------------------------------------------------
function startTimer() {
    updateTimerDisplay();
    timerInterval = setInterval(() => {
        timeRemainingSeconds--;
        updateTimerDisplay();
        if (timeRemainingSeconds <= 0) {
            clearInterval(timerInterval);
            if (operationalMode === "CERTIFICATION" && typeof window.logProctorViolation === "function") {
                window.logProctorViolation("timer_expired", "Exam duration time limit expired.");
            }
            autoSubmitExam();
        }
    }, 1000);
}

function updateTimerDisplay() {
    const timerText = document.getElementById("timerText");
    const timerBadge = document.getElementById("examTimer");
    if (!timerText) return;
    const minutes = Math.floor(timeRemainingSeconds / 60);
    const seconds = timeRemainingSeconds % 60;
    timerText.innerText = `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
    if (timeRemainingSeconds <= 60) {
        timerBadge.style.background = "rgba(239, 68, 68, 0.25)";
        timerBadge.style.borderColor = "var(--accent-red)";
        timerBadge.classList.add("text-danger");
        timerBadge.style.animation = "pulse 1s infinite alternate";
    }
}

// -------------------------------------------------------------
// Render Question
// MOCK_TEST: if answer already given, show correct/wrong highlights + explanation
// CERTIFICATION: normal locked view (no answer reveal until result page)
// -------------------------------------------------------------
function renderQuestion() {
    const card = document.getElementById("questionCard");
    if (!card || questions.length === 0) return;

    const q = questions[currentQuestionIndex];
    const selected = userAnswers[q.id];
    const isMock = (operationalMode === "MOCK_TEST");
    const alreadyAnswered = (selected !== null);

    // Build option classes
    function optionClass(letter) {
        if (!isMock || !alreadyAnswered) return "";
        if (letter === q.correct_option) return "option-correct";
        if (letter === selected && selected !== q.correct_option) return "option-wrong";
        return "";
    }

    const optionLetters = ["A", "B", "C", "D"];
    const optionValues = [q.option_a, q.option_b, q.option_c, q.option_d];
    const optionsHTML = optionLetters.map((letter, i) => `
        <div class="option-box ${selected === letter ? 'selected' : ''} ${optionClass(letter)}"
             onclick="selectOption('${letter}', this)">
            <div class="option-indicator">${letter}</div>
            <div class="option-text text-white-50">${escapeHTML(optionValues[i])}</div>
        </div>
    `).join('');

    // Explanation block — MOCK_TEST only, shown after answering
    let explanationHTML = "";
    if (isMock && alreadyAnswered && q.explanation) {
        const resultLabel = (selected === q.correct_option)
            ? `<span style="color:var(--accent-green);font-weight:600;">✓ Correct</span>`
            : `<span style="color:var(--accent-red);font-weight:600;">✗ Incorrect — correct answer: ${q.correct_option}</span>`;
        explanationHTML = `
            <div class="mock-explanation-box mt-3 p-3"
                 style="background:rgba(99,102,241,0.08);border:1px solid rgba(99,102,241,0.25);border-radius:8px;">
                <div class="mb-2">${resultLabel}</div>
                <div class="text-secondary" style="font-size:0.88rem;line-height:1.6;">
                    <strong>Explanation:</strong> ${escapeHTML(q.explanation)}
                </div>
            </div>
        `;
    }

    card.innerHTML = `
        <div class="question-container">
            <span class="badge bg-primary bg-opacity-10 text-primary border border-primary border-opacity-20 px-3 py-2 mb-3">
                Question ${currentQuestionIndex + 1} of ${questions.length}
                ${isMock ? '<span class="ms-2 text-secondary" style="font-size:0.8rem;">(Practice)</span>' : ''}
            </span>
            <h4 class="text-white mb-4 lh-base">${escapeHTML(q.question_text)}</h4>
            <div class="options-container">
                ${optionsHTML}
            </div>
            ${explanationHTML}
        </div>
    `;

    document.getElementById("progressText").innerText = `Question ${currentQuestionIndex + 1} of ${questions.length}`;
    const pct = ((currentQuestionIndex + 1) / questions.length) * 100;
    document.getElementById("progressBar").style.width = `${pct}%`;
    document.getElementById("prevBtn").disabled = (currentQuestionIndex === 0);

    if (currentQuestionIndex === questions.length - 1) {
        document.getElementById("nextBtn").style.display = "none";
        document.getElementById("submitBtn").style.display = "block";
    } else {
        document.getElementById("nextBtn").style.display = "block";
        document.getElementById("nextBtn").disabled = false;
        document.getElementById("submitBtn").style.display = "none";
    }
}

// -------------------------------------------------------------
// Select Option
// MOCK_TEST: shows instant feedback; answer cannot be changed after selection
// CERTIFICATION: normal selection, can change before submit
// -------------------------------------------------------------
function selectOption(option, el) {
    const q = questions[currentQuestionIndex];
    const isMock = (operationalMode === "MOCK_TEST");

    userAnswers[q.id] = option;

    if (!isMock) {
        // CERTIFICATION: just highlight selected
        document.querySelectorAll(".option-box").forEach(box => box.classList.remove("selected"));
        if (el) el.classList.add("selected");
    }

    syncAnswersToServer();

    // MOCK_TEST: re-render immediately to show correct/wrong + explanation
    if (isMock) {
        renderQuestion();
    }
}

function syncAnswersToServer() {
    if (!window.attemptId) return;
    const payload = Object.keys(userAnswers).map(qId => ({
        question_id: parseInt(qId),
        selected_option: userAnswers[qId]
    }));
    fetch(`/api/proxy/attempts/${window.attemptId}/answers`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ answers: payload })
    })
        .then(res => { if (!res.ok) console.error("Failed to sync progress"); })
        .catch(err => console.error("Sync error:", err));
}

// Button Events
document.getElementById("prevBtn").addEventListener("click", () => {
    if (currentQuestionIndex > 0) {
        currentQuestionIndex--;
        renderQuestion();
    }
});

document.getElementById("nextBtn").addEventListener("click", () => {
    if (currentQuestionIndex < questions.length - 1) {
        currentQuestionIndex++;
        renderQuestion();
    }
});

// -------------------------------------------------------------
// Submission - FIXED with Premium Modal
// -------------------------------------------------------------
function confirmManualSubmit() {
    const unanswered = Object.values(userAnswers).filter(ans => ans === null).length;

    let message = "Are you sure you want to submit?";
    if (unanswered > 0) {
        message = `You have ${unanswered} unanswered question(s). Submit anyway?`;
    }

    // Use premium modal instead of confirm
    if (typeof showPremiumModal === 'function') {
        showPremiumModal(
            message,
            'Submit Exam?',
            unanswered > 0 ? 'warning' : 'info',
            'Submit'
        ).then((confirmed) => {
            if (confirmed) {
                submitExamFlow();
            }
        });
    } else {
        if (confirm(message)) {
            submitExamFlow();
        }
    }
}

function autoSubmitExam() {
    const card = document.getElementById("questionCard");
    card.innerHTML = `
        <div class="text-center py-4">
            <i class="bi bi-clock-fill text-danger" style="font-size: 3rem;"></i>
            <h4 class="mt-3 text-white">Time Expired</h4>
            <p class="text-secondary">Saving your responses...</p>
        </div>
    `;
    submitExamFlow(true);
}

function autoSubmitViolations() {
    const card = document.getElementById("questionCard");
    card.innerHTML = `
        <div class="text-center py-4">
            <i class="bi bi-shield-slash-fill text-danger" style="font-size: 3rem;"></i>
            <h4 class="mt-3 text-white">Rule Violations Exceeded</h4>
            <p class="text-secondary">Saving your responses and auto-submitting due to proctoring infractions...</p>
        </div>
    `;
    submitExamFlow(true);
}
window.autoSubmitViolations = autoSubmitViolations;

function submitExamFlow(isAuto = false) {
    // Prevent proctor events firing during intentional teardown
    window.isExamSubmitting = true;

    // Remove beforeunload listener (CERTIFICATION only, but harmless for MOCK_TEST)
    if (typeof window.removeBeforeUnloadHandler === "function") {
        window.removeBeforeUnloadHandler();
    }

    clearInterval(timerInterval);

    const card = document.getElementById("questionCard");
    card.innerHTML = `
        <div class="text-center py-5">
            <div class="spinner-border text-primary" role="status">
                <span class="visually-hidden">Loading...</span>
            </div>
            <p class="text-secondary mt-3 mb-0" id="submitStatusText">Stopping webcam recording...</p>
        </div>
    `;

    // Webcam recording stops and uploads for BOTH CERTIFICATION and MOCK_TEST
    const stopRecordingPromise = (typeof window.stopExamRecording === "function")
        ? window.stopExamRecording()
        : Promise.resolve(null);

    stopRecordingPromise
        .then(videoBlob => {
            if (!videoBlob) {
                console.log("No recording blob — skipping upload.");
                return proceedToSubmit(card);
            }

            console.log("Recording blob size:", videoBlob.size, "bytes");
            const statusText = document.getElementById("submitStatusText");
            if (statusText) statusText.innerText = "Uploading session recording...";

            const formData = new FormData();
            formData.append("file", videoBlob, `attempt_${window.attemptId}.webm`);

            return fetch(`/api/proxy/attempts/${window.attemptId}/upload-recording`, {
                method: "POST",
                body: formData
            })
                .then(res => {
                    if (!res.ok) console.warn("Recording upload returned status:", res.status);
                    else console.log("Recording upload complete.");
                })
                .catch(err => console.error("Recording upload failed (non-blocking):", err))
                .then(() => proceedToSubmit(card));
        })
        .catch(err => {
            console.error("Error stopping recording, submitting anyway:", err);
            return proceedToSubmit(card);
        });
}

function proceedToSubmit(card) {
    const statusText = document.getElementById("submitStatusText");
    if (statusText) statusText.innerText = "Grading and saving result...";

    return fetch(`/api/proxy/attempts/${window.attemptId}/submit`, { method: "POST" })
        .then(res => {
            if (!res.ok) throw new Error("Grading connection error");
            return res.json();
        })
        .then(data => {
            // Exit fullscreen if CERTIFICATION mode entered it
            if (document.fullscreenElement) {
                document.exitFullscreen().catch(err => console.log("Exit fullscreen:", err));
            }

            // Show success toast before redirect
            if (typeof showPremiumToast === 'function') {
                showPremiumToast('✅ Exam submitted successfully! Redirecting to results...', 'success');
            }

            setTimeout(() => {
                window.location.href = `/exam/attempt/${window.attemptId}/result`;
            }, 300);
        })
        .catch(err => {
            window.isExamSubmitting = false;
            card.innerHTML = `
                <div class="text-center py-4">
                    <i class="bi bi-exclamation-circle text-danger" style="font-size: 3rem;"></i>
                    <h4 class="mt-3 text-white">Submission Error</h4>
                    <p class="text-secondary">${escapeHTML(err.message)}</p>
                    <button class="btn btn-premium mt-3" onclick="window.location.href='/candidate/dashboard'">Return to Dashboard</button>
                </div>
            `;
            if (typeof showPremiumToast === 'function') {
                showPremiumToast(`❌ Submission failed: ${err.message}`, 'error');
            }
        });
}

function exitAndReturn() {
    window.isExamSubmitting = true;
    if (document.fullscreenElement) {
        document.exitFullscreen().catch(e => console.log(e));
    }
    window.location.href = "/candidate/dashboard";
}

// HTML escape helper
function escapeHTML(str) {
    if (!str) return "";
    return str
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}