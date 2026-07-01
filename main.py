// ==UserScript==
// @name         Follow → Close → Verify Loop (Start/Stop)
// @namespace    http://tampermonkey.net/
// @version      1.0
// @description  Loop: click Follow → close popup → verify → repeat with Start/Stop UI
// @match        *://*/*
// @grant        none
// @run-at       document-idle
// ==/UserScript==

(function () {
    'use strict';

    // Default selectors — edit in the floating UI if needed
    const defaults = {
        followSelector: '#earnBtn, a#earnBtn, .follow, .btn-follow, button[data-action="follow"], #follow',
        closeSelector: '.close, .modal-close, .popup-close, #close',
        verifySelector: '#verifybtn, .verify, button.verify',
        clickDelay: 600, // delay between actions (ms)
        popupCloseDelay: 900,
        loopDelay: 1200 // wait before starting next iteration
    };

    let running = localStorage.getItem('tm_loop_running') === 'true';

    function log(...args) { console.log('[TM Loop]', ...args); }

    // Utility: wait ms
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));

    // Utility: wait for selector with timeout
    async function waitForSelector(selector, timeout = 3000, interval = 250) {
        const stop = Date.now() + timeout;
        while (Date.now() < stop) {
            const el = document.querySelector(selector);
            if (el) return el;
            await sleep(interval);
        }
        return null;
    }

    // Click safely
    function clickEl(el) {
        try {
            el.focus && el.focus();
            el.click();
            return true;
        } catch (e) {
            try {
                const ev = new MouseEvent('click', { bubbles: true, cancelable: true, view: window });
                el.dispatchEvent(ev);
                return true;
            } catch (err) {
                console.error(err);
                return false;
            }
        }
    }

    // Hook window.open to auto-close popups when running
    (function hookOpen() {
        const w = window;
        const origOpen = w.open;
        w.open = function (...args) {
            const popup = origOpen.apply(this, args);
            if (!running) return popup;
            if (popup) {
                log('popup opened — will attempt to close');
                setTimeout(() => {
                    try { if (!popup.closed) popup.close(); } catch (e) { console.warn(e); }
                }, defaults.popupCloseDelay);
            }
            return popup;
        };
    })();

    // Main sequence: click follow -> (popup opens and gets closed by hook) -> click close (if present) -> click verify
    async function performSequence(config) {
        // Click follow
        const follow = await waitForSelector(config.followSelector, 1500, 200);
        if (!follow) { log('Follow button not found:', config.followSelector); return; }
        log('Clicking Follow'); clickEl(follow);

        await sleep(config.clickDelay);

        // Try to click close (in case of modal in same window)
        const closeBtn = document.querySelector(config.closeSelector);
        if (closeBtn) {
            log('Clicking Close'); clickEl(closeBtn);
        } else {
            log('Close button not found; relying on popup hook');
        }

        await sleep(config.popupCloseDelay);

        // Click verify
        const verify = await waitForSelector(config.verifySelector, 1500, 200);
        if (verify) {
            // If the verify button is disabled, try to enable it before clicking
            try {
                if (verify.disabled) {
                    verify.disabled = false;
                    verify.removeAttribute && verify.removeAttribute('disabled');
                    log('Enabled verify button');
                }
            } catch (e) {
                console.warn('Could not enable verify button', e);
            }

            log('Clicking Verify'); clickEl(verify);
        } else {
            log('Verify button not found:', config.verifySelector);
        }
    }

    // Loop runner
    let loopHandle = null;

    async function loopRunner(config) {
        log('Loop started');
        while (running) {
            try {
                await performSequence(config);
            } catch (e) {
                console.error(e);
            }
            await sleep(config.loopDelay);
        }
        log('Loop stopped');
        loopHandle = null;
    }

    // Floating control panel
    function createPanel() {
        const panel = document.createElement('div');
        panel.id = 'tm-loop-panel';
        Object.assign(panel.style, {
            position: 'fixed', right: '16px', top: '16px', zIndex: 1e6,
            background: 'rgba(0,0,0,0.8)', color: '#fff', padding: '10px',
            borderRadius: '8px', fontFamily: 'Arial, sans-serif', fontSize: '13px',
            minWidth: '220px', boxShadow: '0 6px 18px rgba(0,0,0,0.4)'
        });

        const title = document.createElement('div');
        title.textContent = 'Follow→Close→Verify';
        title.style.fontWeight = '700';
        title.style.marginBottom = '8px';

        const startBtn = document.createElement('button');
        startBtn.textContent = running ? 'Stop' : 'Start';
        Object.assign(startBtn.style, { padding: '6px 10px', marginRight: '8px' });
        startBtn.addEventListener('click', () => {
            running = !running;
            localStorage.setItem('tm_loop_running', running);
            startBtn.textContent = running ? 'Stop' : 'Start';
            if (running && !loopHandle) loopHandle = loopRunner(getConfig());
        });

        const status = document.createElement('span');
        status.textContent = running ? 'Running' : 'Stopped';
        status.style.marginLeft = '6px';

        // Inputs for selectors
        function labeledInput(labelText, key, placeholder) {
            const wrap = document.createElement('div');
            wrap.style.marginTop = '6px';
            const label = document.createElement('div'); label.textContent = labelText; label.style.fontSize = '11px';
            const inp = document.createElement('input'); inp.type = 'text'; inp.placeholder = placeholder;
            inp.style.width = '100%'; inp.style.marginTop = '4px'; inp.dataset.key = key;
            inp.value = localStorage.getItem('tm_cfg_' + key) || defaults[key];
            wrap.appendChild(label); wrap.appendChild(inp);
            return wrap;
        }

        const followInput = labeledInput('Follow selector', 'followSelector', defaults.followSelector);
        const closeInput = labeledInput('Close selector', 'closeSelector', defaults.closeSelector);
        const verifyInput = labeledInput('Verify selector', 'verifySelector', defaults.verifySelector);

        const saveBtn = document.createElement('button');
        saveBtn.textContent = 'Save';
        saveBtn.style.marginTop = '8px';
        saveBtn.addEventListener('click', () => {
            [followInput, closeInput, verifyInput].forEach(w => {
                const input = w.querySelector('input');
                localStorage.setItem('tm_cfg_' + input.dataset.key, input.value.trim());
            });
            log('Configuration saved');
        });

        // Small log area
        const logArea = document.createElement('div');
        Object.assign(logArea.style, { marginTop: '8px', maxHeight: '90px', overflow: 'auto', fontSize: '11px', color: '#eee' });

        // Update status periodically
        setInterval(() => { status.textContent = running ? 'Running' : 'Stopped'; }, 500);

        panel.appendChild(title);
        panel.appendChild(startBtn);
        panel.appendChild(status);
        panel.appendChild(followInput);
        panel.appendChild(closeInput);
        panel.appendChild(verifyInput);
        panel.appendChild(saveBtn);
        panel.appendChild(logArea);

        document.body.appendChild(panel);

        // Mirror logs to panel
        const origConsoleLog = console.log;
        console.log = function (...args) { origConsoleLog.apply(console, args); appendToLog(args.join(' ')); };

        function appendToLog(text) {
            const line = document.createElement('div'); line.textContent = text; logArea.appendChild(line); logArea.scrollTop = logArea.scrollHeight;
        }
    }

    function getConfig() {
        return {
            followSelector: localStorage.getItem('tm_cfg_followSelector') || defaults.followSelector,
            closeSelector: localStorage.getItem('tm_cfg_closeSelector') || defaults.closeSelector,
            verifySelector: localStorage.getItem('tm_cfg_verifySelector') || defaults.verifySelector,
            clickDelay: defaults.clickDelay,
            popupCloseDelay: defaults.popupCloseDelay,
            loopDelay: defaults.loopDelay
        };
    }

    // Start UI when DOM ready
    function init() {
        if (!document.body) return requestAnimationFrame(init);
        createPanel();
        if (running && !loopHandle) loopHandle = loopRunner(getConfig());
    }

    init();

})();
